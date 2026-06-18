"""
RTCM Monitor — 录制底座 (第二期 / 项3)。

按 UTC 整 10 分钟为一个周期 (00:00–00:10, 00:10–00:20, …)，每个周期只保留
**最后一个完整历元** 的全部 RTCM 报文，外加该历元时刻每颗卫星的位置快照
(az/el/cn0)，边产生边落盘，断电/休眠/重启后仍可在第三期按时间段导出。

完整历元的边界用 MSM 的 "Multiple Message Bit" (DF393) 识别：一个观测历元的
所有 MSM 报文里，除最后一条外该位都为 1，最后一条为 0。因此跨星座 (GPS+GAL+
BDS+GLO…) 的同一历元会被正确归并为一组。两个历元之间夹带的非 MSM 报文
(如 1005 基准站坐标) 会一并附在随后的历元里。

落盘目录结构::

    recordings/<sid>/<role>__<mount>/
        20260617T0930Z.rtcm    该周期最后一个完整历元的原始字节(可回放复算)
        20260617T0930Z.csv     该历元卫星位置快照: time_utc,sat,sys,prn,az,el,cn0
        index.jsonl            每封存一个周期追加一行(供第三期按时间段检索)

设计要点
--------
* **连接即录**：随数据源启动，无需手动开关。
* **每周期一个完整历元**：周期内每来一个新历元就覆盖候选，设备在第 9 分钟休眠
  时，盘上留存的就是休眠前最近的那个完整历元。
* **节流写盘**：周期内最多每 ``WRITE_THROTTLE`` 秒覆盖一次，周期切换时强制落盘并
  封存上一周期，避免 1Hz 频繁写小文件。
* **锁次序**：卫星快照回调 ``sat_snapshot_fn`` 会去拿 State 的锁，因此一律在持有
  录制器自身锁 **之前** 调用，避免与 ``snapshot()`` 形成交叉死锁。
"""
import json
import os
import re
import threading
import time

PERIOD_SEC = 600          # 周期长度：10 分钟
WRITE_THROTTLE = 15.0     # 同一周期内最多每 N 秒覆盖写一次候选历元
MAX_BUF_FRAMES = 5000     # 单历元缓冲上限(防误接非观测流时无限增长)


def _stamp(period_start):
    """UTC 周期起点 -> 文件名/索引用的时间戳, 如 20260617T0930Z。"""
    return time.strftime("%Y%m%dT%H%MZ", time.gmtime(period_start))


def _safe(name):
    """挂载点/角色名清洗成可做目录名。"""
    s = re.sub(r"[^0-9A-Za-z._-]+", "_", str(name or "")).strip("_")
    return s or "unknown"


def _msg_tow(msg):
    """从一条已解析的 MSM 报文里取历元时间 (ms)，取不到返回 None。"""
    for df in ("DF004", "DF427", "DF248", "DF428", "DF034", "DF546"):
        v = getattr(msg, df, None)
        if v is not None:
            return v
    return None


def _atomic_write(path, data: bytes):
    """临时文件 + rename, 避免读到写一半的文件。"""
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


class Recorder:
    """单个通道 (base/rover) 的录制器。线程安全 (数据源线程调用 feed)。"""

    def __init__(self, base_dir, sid, role, label, mountpoint,
                 sat_snapshot_fn, logger=None):
        self.sid = sid
        self.role = role
        self.label = label
        self.mount = mountpoint
        self._snap = sat_snapshot_fn          # () -> [{sat,sys,prn,az,el,cn0}, ...]
        self._log = logger or (lambda level, msg: None)
        self._lock = threading.Lock()

        self.dir = os.path.join(base_dir, _safe(sid),
                                f"{_safe(role)}__{_safe(mountpoint or 'file')}")
        os.makedirs(self.dir, exist_ok=True)
        self.index_path = os.path.join(self.dir, "index.jsonl")

        # 当前正在组装的历元 (受 _lock 保护)
        self._buf = []                # 原始帧字节列表
        self._buf_has_msm = False
        self._buf_tow = None
        # 当前周期的候选历元, 会被周期内更晚的历元覆盖 (受 _lock 保护)
        self._cur = None              # {start, frames, tow, nframes, sats, ts, dirty}
        self._last_write = 0.0

        self.periods_saved = 0
        self.last_epoch_ts = None
        self.last_error = None

    # ---- 数据源线程: 每收到一条非星历帧调用一次 -------------------------
    def feed(self, raw, msg, now=None):
        """raw: 原始帧字节; msg: 已 pyrtcm 解析的报文对象。"""
        now = now if now is not None else time.time()
        df393 = getattr(msg, "DF393", None)   # 仅 MSM 报文有该字段
        epoch = None
        with self._lock:
            self._buf.append(raw)
            # 安全阀: 若长时间收不到完整历元(如误把纯1005/纯星历挂载点当观测源),
            # 丢弃积压, 避免缓冲无限增长。
            if len(self._buf) > MAX_BUF_FRAMES:
                self._buf = []
                self._buf_has_msm = False
                self._buf_tow = None
            if df393 is not None:
                self._buf_has_msm = True
                t = _msg_tow(msg)
                if t is not None:
                    self._buf_tow = t
                if df393 == 0:                # 历元最后一条 -> 完整历元就绪
                    epoch = (b"".join(self._buf), self._buf_tow, len(self._buf))
                    self._buf = []
                    self._buf_has_msm = False
                    self._buf_tow = None
        if epoch is not None:
            self._on_epoch(now, *epoch)

    # ---- 完整历元就绪 (不持 _lock 时调用, 先取卫星快照再进锁) -----------
    def _on_epoch(self, now, frames, tow, nframes):
        try:
            sats = self._snap() or []         # 会去拿 State 锁 -> 必须在 _lock 外
        except Exception:
            sats = []
        period_start = int(now // PERIOD_SEC) * PERIOD_SEC
        try:
            with self._lock:
                if self._cur is None:
                    self._cur = {"start": period_start}
                elif period_start != self._cur["start"]:
                    # 周期切换: 上一周期最终候选写盘并封存, 再开新周期
                    self._write(self._cur, force=True)
                    self._seal(self._cur)
                    self.periods_saved += 1
                    self._cur = {"start": period_start}
                    self._last_write = 0.0
                self._cur.update(frames=frames, tow=tow, nframes=nframes,
                                 sats=sats, ts=now, dirty=True)
                self.last_epoch_ts = now
                self._write(self._cur, force=False)
        except Exception as e:                # 录制不应影响主解码链路
            self.last_error = str(e)
            self._log("warn", f"[{self.label}] 录制写盘异常: {e}")

    # ---- 覆盖写当前周期候选 (调用方须持 _lock) -------------------------
    def _write(self, cur, force):
        if not cur or not cur.get("dirty"):
            return
        now = time.time()
        if not force and (now - self._last_write) < WRITE_THROTTLE:
            return
        stamp = _stamp(cur["start"])
        base = os.path.join(self.dir, stamp)
        _atomic_write(base + ".rtcm", cur["frames"])   # 历元原始报文(可回放)
        iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cur["ts"]))
        rows = []
        for s in cur.get("sats", []):
            rows.append("{t},{sat},{sys},{prn},{az},{el},{cn0}".format(
                t=iso, sat=s.get("sat", ""), sys=s.get("sys", ""),
                prn=s.get("prn", ""),
                az="" if s.get("az") is None else round(s["az"], 2),
                el="" if s.get("el") is None else round(s["el"], 2),
                cn0="" if s.get("cn0") is None else s["cn0"]))
        csv = "time_utc,sat,sys,prn,az_deg,el_deg,cn0\n" + "\n".join(rows)
        csv = csv + ("\n" if rows else "")
        _atomic_write(base + ".csv", csv.encode("utf-8"))
        cur["nsat"] = len(rows)
        cur["dirty"] = False
        self._last_write = now

    def _seal(self, cur):
        """周期封存: 向 index.jsonl 追加一行 (第三期按时间段检索用)。"""
        line = {
            "period_start": cur["start"],
            "stamp": _stamp(cur["start"]),
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                 time.gmtime(cur["start"])),
            "tow_ms": cur.get("tow"),
            "nframes": cur.get("nframes"),
            "nbytes": len(cur.get("frames", b"")),
            "nsat": cur.get("nsat"),
            "role": self.role,
            "mount": self.mount,
        }
        with open(self.index_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")

    # ---- 数据源停止时调用: 落盘并封存当前周期 --------------------------
    def close(self):
        with self._lock:
            try:
                if self._cur and self._cur.get("frames"):
                    self._write(self._cur, force=True)
                    self._seal(self._cur)
                    self.periods_saved += 1
                self._cur = None
            except Exception as e:
                self.last_error = str(e)

    def status(self):
        with self._lock:
            cur = self._cur
            return {
                "recording": True,
                "dir": self.dir,
                "periods": self.periods_saved,
                "cur_period": _stamp(cur["start"]) if cur else None,
                "cur_tow": cur.get("tow") if cur else None,
                "cur_frames": cur.get("nframes") if cur else None,
                "last_epoch_ts": self.last_epoch_ts,
                "error": self.last_error,
            }


def list_recordings(base_dir, sid):
    """列出某会话已封存的录制 (供前端/第三期导出概览)。"""
    root = os.path.join(base_dir, _safe(sid))
    out = {}
    if not os.path.isdir(root):
        return out
    for d in sorted(os.listdir(root)):
        idx = os.path.join(root, d, "index.jsonl")
        periods = []
        if os.path.isfile(idx):
            with open(idx, encoding="utf-8") as fh:
                for ln in fh:
                    ln = ln.strip()
                    if ln:
                        try:
                            periods.append(json.loads(ln))
                        except Exception:
                            pass
        out[d] = {"periods": len(periods), "items": periods}
    return out
