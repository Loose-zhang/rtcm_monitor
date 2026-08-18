"""
RTCM Monitor — web dashboard backend.

Connects to an NTRIP caster (or replays a local .rtcm file), decodes RTCM3 MSM
observations, aggregates per-satellite / per-signal CN0 + frequency, applies the
BeiDou 2026/04 reconstruction-aware checks, and streams it all to the browser
dashboard over Server-Sent Events.

Run:  python app.py   ->  open http://127.0.0.1:7999
"""
import argparse
import json
import math
import os
import statistics
import threading
import time
from collections import defaultdict, deque

from flask import Flask, Response, request, send_from_directory

import rtcm_decode as rd
import bds_recon as br
import gnss_orbit as orb
import gnss_spp as spp
import recorder as rec
import exporter as exp

STALE_SEC = 15.0          # drop a signal not refreshed for this long
# 录制底座落盘根目录: 项目下 recordings/ (与 app.py 同级)
RECORD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recordings")
# 第三期导出根目录: 项目下 exports/
EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
app = Flask(__name__, static_folder="static")


def _msg_name(ident):
    """Human-readable name for an RTCM message type."""
    names = {
        "1005": "基准站坐标(ARP)", "1006": "基准站坐标+天线高",
        "1019": "GPS 星历", "1020": "GLONASS 星历", "1042": "BDS 星历",
        "1044": "QZSS 星历", "1046": "Galileo 星历(I/NAV)",
        "1045": "Galileo 星历(F/NAV)", "1033": "接收机/天线描述",
        "1230": "GLONASS 码相位偏差",
    }
    if ident in names:
        return names[ident]
    try:
        n = int(ident)
        sysn = {107: "GPS", 108: "GLONASS", 109: "Galileo",
                111: "QZSS", 112: "BDS"}.get(n // 10)
        if sysn:
            return f"{sysn} MSM{n % 10} 观测值"
    except ValueError:
        pass
    return "RTCM 报文"


class Channel:
    """One observation stream (a NTRIP / file source) and its decoded state.
    Two channels live in a State: 'base' (基站) and 'rover' (测站)."""

    def __init__(self, label):
        self.label = label                     # "基站" / "测站"
        self.source = None
        self.recorder = None                   # 录制底座 (连接即录)
        self.status = {"state": "idle", "msg": "未连接"}
        # sat -> {sys,prn, signals:{code:{...,'ts':t}}}
        self.sats = defaultdict(lambda: {"signals": {}})
        self.station = None
        self.tow_ms = None
        self.tow_ms_by_sys = {}
        self.last_sysname = None
        self.msg_count = 0
        self.msg_times = []                     # timestamps for rate calc
        self.seen_types = set()                 # RTCM idents already announced
        self._active_sats = set()               # for appear/lost detection
        self._recon_flagged = {}                # sat -> set(anomaly msgs logged)

    @property
    def mountpoint(self):
        return getattr(self.source, "mountpoint", None)

    def reset(self):
        self.sats = defaultdict(lambda: {"signals": {}})
        self.station = None
        self.tow_ms = None
        self.tow_ms_by_sys = {}
        self.last_sysname = None
        self.msg_count = 0
        self.msg_times = []
        self.seen_types = set()
        self._active_sats = set()
        self._recon_flagged = {}


class State:
    def __init__(self, sid="default"):
        self.sid = sid
        self.lock = threading.Lock()
        # two observation channels: base (基站) + rover (测站)
        self.channels = {"base": Channel("基站"), "rover": Channel("测站")}
        self.base_xyz = None         # base ECEF from RTCM or automatic SPP
        self.base_xyz_role = None    # which channel supplied base_xyz
        self.base_xyz_source = None  # RTCM1005/1006 or SPP-<system>
        self.base_xyz_quality = None
        self._spp_history = defaultdict(lambda: deque(maxlen=12))
        self._spp_last_epoch = {}
        # broadcast ephemeris (separate stream, shared by both channels)
        self.eph_source = None
        self.eph = {}                # sat -> normalized eph (+ 'ts')
        self.eph_msg_count = 0
        self.eph_status = {"state": "idle", "msg": "未连接"}
        # ---- event log (ring buffer, sent to browser) ----
        self.events = deque(maxlen=200)
        self._last_tick = 0.0

    def log(self, level, msg):
        """level: info / success / warn / error"""
        self.events.append({"ts": time.time(), "level": level, "msg": msg})

    def _clear_base_position_locked(self):
        self.base_xyz = None
        self.base_xyz_role = None
        self.base_xyz_source = None
        self.base_xyz_quality = None
        self._spp_history = defaultdict(lambda: deque(maxlen=12))
        self._spp_last_epoch = {}

    def _prepare_spp_locked(self, role, system, tow_ms):
        """Copy one epoch's best pseudorange per satellite for lock-free SPP."""
        if role != "base" or tow_ms is None or system not in orb.CONST:
            return None
        # A standard reference-station coordinate always outranks coarse SPP.
        if self.base_xyz_source and self.base_xyz_source.startswith("RTCM"):
            return None
        ch = self.channels[role]
        observations = []
        eph = {}
        for sat, sv in ch.sats.items():
            if sv.get("sys") != system:
                continue
            candidates = [v for v in sv["signals"].values()
                          if v.get("tow_ms") == tow_ms
                          and v.get("pseudorange") is not None]
            if not candidates:
                continue
            best = max(candidates, key=lambda v: v.get("cn0") or 0)
            observations.append({"sat": sat, "pseudorange": best["pseudorange"],
                                 "cn0": best.get("cn0")})
            if sat in self.eph:
                eph[sat] = dict(self.eph[sat])
        if len(observations) < spp.MIN_SATS:
            if self.base_xyz is None:
                self.base_xyz_quality = {
                    "state": "waiting", "system": system,
                    "reason": "有效伪距不足", "observations": len(observations),
                    "required": spp.MIN_SATS,
                }
            return None
        if len(eph) < spp.MIN_SATS:
            if self.base_xyz is None:
                self.base_xyz_quality = {
                    "state": "waiting", "system": system,
                    "reason": "匹配广播星历不足", "observations": len(observations),
                    "ephemerides": len(eph), "required": spp.MIN_SATS,
                }
            return None
        initial = self.base_xyz if (self.base_xyz_source or "").startswith("SPP-") else None
        return observations, eph, tow_ms, system, initial

    def _run_spp(self, job):
        observations, eph, tow_ms, system, initial = job
        solution = spp.solve(observations, eph, tow_ms, system, initial)
        if solution is None:
            with self.lock:
                if self.base_xyz is None:
                    self.base_xyz_quality = {
                        "state": "rejected", "system": system,
                        "reason": "星历过期、卫星几何或伪距残差未通过校验",
                        "observations": len(observations),
                        "ephemerides": len(eph),
                    }
            return
        with self.lock:
            if self.base_xyz_source and self.base_xyz_source.startswith("RTCM"):
                return
            active_system = (self.base_xyz_source or "").removeprefix("SPP-")
            if (self.base_xyz_source or "").startswith("SPP-") \
                    and active_system != system:
                return
            if self._spp_last_epoch.get(system) == tow_ms:
                return
            self._spp_last_epoch[system] = tow_ms
            hist = self._spp_history[system]
            hist.append(solution)
            recent = list(hist)[-5:]
            self.base_xyz_quality = {
                "state": "collecting" if len(recent) < 3 else "checking",
                "system": system, "samples": len(recent),
                "nsat": solution["nsat"], "rms_m": solution["rms_m"],
            }
            if len(recent) < 3:
                return
            xyz = tuple(statistics.median(s["xyz"][i] for s in recent)
                        for i in range(3))
            spread = max(math.sqrt(sum((s["xyz"][i] - xyz[i]) ** 2
                                       for i in range(3))) for s in recent)
            if spread > 500.0:
                self.base_xyz_quality.update({"state": "unstable",
                                              "spread_m": round(spread, 1)})
                return
            first = not (self.base_xyz_source or "").startswith("SPP-")
            self.base_xyz = xyz
            self.base_xyz_role = "base"
            self.base_xyz_source = f"SPP-{system}"
            self.base_xyz_quality.update({"state": "stable",
                                          "spread_m": round(spread, 1)})
            if first:
                self.log("success", f"已由{system}系统MSM伪距+广播星历自动解算基站坐标 "
                         f"({solution['nsat']}星, RMS {solution['rms_m']}m)")

    # ---- called from a channel's source thread, per validated frame
    def on_frame(self, frame, role):
        ch = self.channels[role]
        try:
            msg = rd.RTCMReader.parse(frame)
        except Exception:
            return
        ident = str(msg.identity)
        now = time.time()
        with self.lock:
            if ident not in ch.seen_types:
                ch.seen_types.add(ident)
                self.log("info", f"[{ch.label}] 首次收到 {ident} 报文 ({_msg_name(ident)})")
        if ident in orb.EPH_TYPES:                 # ephemeris piggy-backed on obs
            e = orb.normalize(msg)
            with self.lock:
                self.eph_msg_count += 1
                if e:
                    e["ts"] = now
                    self.eph[e["sat"]] = e
            return
        # 录制底座: 喂入非星历帧 (用 DF393 组装完整历元, 与主解码并行)
        if ch.recorder is not None:
            ch.recorder.feed(frame, msg, now)
        d = rd.decode_msm(msg)
        with self.lock:
            ch.msg_count += 1
            ch.msg_times.append(now)
            ch.msg_times = ch.msg_times[-200:]
            if ident in ("1005", "1006"):   # station ARP (+ optional height)
                sid = getattr(msg, "DF003", None)
                if sid is not None and sid != ch.station:
                    self.log("info", f"[{ch.label}] 基准站 ID: {sid}")
                ch.station = sid
                x = getattr(msg, "DF025", None)
                y = getattr(msg, "DF026", None)
                z = getattr(msg, "DF027", None)
                if None not in (x, y, z):
                    # base supplies the shared coordinate; rover only fills in
                    # when base hasn't provided one yet (rover shares base coord).
                    if role == "base" or self.base_xyz is None:
                        if self.base_xyz is None:
                            self.log("success", "已获取基准站 ECEF 坐标，天空图可用真实方位/高度角")
                        self.base_xyz = (x, y, z)
                        self.base_xyz_role = role
                        self.base_xyz_source = f"RTCM{ident}-{role}"
                        self.base_xyz_quality = {"state": "authoritative"}
            if not d:
                return
            ch.station = d["station"]
            ch.tow_ms = d["tow_ms"]
            ch.tow_ms_by_sys[d["sys"]] = d["tow_ms"]
            ch.last_sysname = d["sysname"]
            for c in d["cells"]:
                s = ch.sats[c["sat"]]
                s["sys"] = c["sys"]
                s["prn"] = c["prn"]
                s["signals"][c["code"]] = {
                    "code": c["code"], "signal": c["signal"],
                    "band": c["band"], "band_name": c["band_name"],
                    "freq_mhz": c["freq_mhz"], "cn0": c["cn0"],
                    "pseudorange": c["pseudorange"], "tow_ms": d["tow_ms"],
                    "ts": now,
                }
            spp_job = self._prepare_spp_locked(role, d["sys"], d["tow_ms"])
        if spp_job:
            self._run_spp(spp_job)

    # ---- dedicated ephemeris-stream frame handler (ignores observations)
    def on_eph_frame(self, frame):
        try:
            msg = rd.RTCMReader.parse(frame)
        except Exception:
            return
        ident = str(msg.identity)
        now = time.time()
        if ident not in orb.EPH_TYPES:
            return
        e = orb.normalize(msg)
        with self.lock:
            self.eph_msg_count += 1
            if e:
                e["ts"] = now
                self.eph[e["sat"]] = e

    _ST_LEVEL = {"connecting": "info", "streaming": "success",
                 "reconnecting": "warn", "error": "error",
                 "stopped": "info", "idle": "info"}

    def on_status(self, st, role):
        ch = self.channels[role]
        with self.lock:
            if st.get("state") != ch.status.get("state"):
                self.log(self._ST_LEVEL.get(st.get("state"), "info"),
                         f"[{ch.label}] " + st.get("msg", st.get("state", "")))
            ch.status = st

    def on_eph_status(self, st):
        with self.lock:
            if st.get("state") != self.eph_status.get("state"):
                self.log(self._ST_LEVEL.get(st.get("state"), "info"),
                         f"[星历] {st.get('msg', st.get('state', ''))}")
            self.eph_status = st

    # ---- build one channel's satellite list + per-channel stats
    def _channel_snapshot(self, role, now):
        ch = self.channels[role]
        c_sats = []
        sys_count = defaultdict(int)
        sig_total = 0
        multi_freq = 0
        for sat, s in sorted(ch.sats.items()):
            sigs = [v for v in s["signals"].values()
                    if now - v["ts"] <= STALE_SEC]
            if not sigs:
                continue
            sigs.sort(key=lambda x: (x["band"] or 99))
            recon = None
            if s.get("sys") == "C":
                recon = br.check(s["prn"], [v["code"] for v in sigs])
            cn0s = [v["cn0"] for v in sigs if v["cn0"]]
            az = el = None
            e = self.eph.get(sat)
            # rover shares the base coordinate -> both channels use base_xyz
            sat_tow_ms = ch.tow_ms_by_sys.get(s.get("sys"))
            if e and self.base_xyz and sat_tow_ms is not None \
                    and s.get("sys") == e["sys"]:
                try:
                    p = orb.sat_ecef(e, sat_tow_ms / 1000.0)
                    if p:
                        az, el = orb.azel(self.base_xyz, p)
                except Exception:
                    pass
            c_sats.append({
                "sat": sat, "role": role, "sys": s.get("sys"), "prn": s.get("prn"),
                "signals": [{k: v[k] for k in
                             ("code", "signal", "band", "band_name",
                              "freq_mhz", "cn0", "pseudorange")}
                            for v in sigs],
                "maxcn0": max(cn0s) if cn0s else 0,
                "recon": recon, "az": az, "el": el,
            })
            sys_count[s.get("sys")] += 1
            sig_total += len(sigs)
            if len({v["band"] for v in sigs if v["band"]}) >= 2:
                multi_freq += 1

        times = [t for t in ch.msg_times if now - t <= 5]
        stat = {
            "label": ch.label,
            "status": ch.status,
            "station": ch.station,
            "tow_ms": ch.tow_ms,
            "sysname": ch.last_sysname,
            "mountpoint": ch.mountpoint,
            "sats": len(c_sats),
            "signals": sig_total,
            "multi_freq": multi_freq,
            "msg_rate": round(len(times) / 5.0, 1),
            "msg_count": ch.msg_count,
            "bytes_in": getattr(ch.source, "bytes_in", 0) if ch.source else 0,
            "frames": getattr(ch.source, "frames", 0) if ch.source else 0,
            "rec": ch.recorder.status() if ch.recorder else None,
        }
        return c_sats, stat, dict(sys_count)

    # ---- 录制器回调: 取某通道当前卫星位置快照 (供历元落盘) ----------
    def _sat_positions(self, role):
        now = time.time()
        with self.lock:
            c_sats, _stat, _sc = self._channel_snapshot(role, now)
        return [{"sat": s["sat"], "sys": s["sys"], "prn": s["prn"],
                 "az": s["az"], "el": s["el"], "cn0": s["maxcn0"]}
                for s in c_sats]

    # ---- snapshot for the browser
    def snapshot(self):
        now = time.time()
        with self.lock:
            sats_out = []
            sys_count = defaultdict(int)
            chan_stats = {}
            agg = {"sats": 0, "signals": 0, "multi_freq": 0,
                   "msg_count": 0, "bytes_in": 0, "frames": 0, "rate": 0.0}
            cur_sets = {}
            for role in ("base", "rover"):
                c_sats, stat, sc = self._channel_snapshot(role, now)
                sats_out.extend(c_sats)
                chan_stats[role] = stat
                cur_sets[role] = {so["sat"] for so in c_sats}
                for k, v in sc.items():
                    sys_count[k] += v
                agg["sats"] += stat["sats"]
                agg["signals"] += stat["signals"]
                agg["multi_freq"] += stat["multi_freq"]
                agg["msg_count"] += stat["msg_count"]
                agg["bytes_in"] += stat["bytes_in"]
                agg["frames"] += stat["frames"]
                agg["rate"] += stat["msg_rate"]
            agg["rate"] = round(agg["rate"], 1)

            # ---- 1 Hz event tick (guarded so multiple SSE clients don't dup)
            if now - self._last_tick >= 0.9:
                self._last_tick = now
                for role in ("base", "rover"):
                    ch = self.channels[role]
                    cur = cur_sets[role]
                    gained = sorted(cur - ch._active_sats)
                    lost = sorted(ch._active_sats - cur)
                    if gained:
                        self.log("info", f"[{ch.label}] 新增卫星 {', '.join(gained[:8])}"
                                 + (f" 等{len(gained)}颗" if len(gained) > 8 else ""))
                    if lost:
                        self.log("warn", f"[{ch.label}] 失锁卫星 {', '.join(lost[:8])}"
                                 + (f" 等{len(lost)}颗" if len(lost) > 8 else ""))
                        for s_ in lost:
                            ch._recon_flagged.pop(s_, None)
                    ch._active_sats = cur
                # BDS 重构异常：每通道每星每条只记一次，按(通道,消息)合并
                anom = {}
                for so in sats_out:
                    rc = so.get("recon")
                    if not rc or rc.get("ok"):
                        continue
                    ch = self.channels[so["role"]]
                    flagged = ch._recon_flagged.setdefault(so["sat"], set())
                    for a in rc["anomalies"]:
                        if a["level"] == "info" or a["msg"] in flagged:
                            continue
                        flagged.add(a["msg"])
                        lv, lst = anom.setdefault((so["role"], a["msg"]),
                                                  (a["level"], []))
                        lst.append(so["sat"])
                for (role, m), (lv, lst) in anom.items():
                    lab = self.channels[role].label
                    self.log(lv, f"[{lab}] {', '.join(lst[:8])}"
                             + (f" 等{len(lst)}颗" if len(lst) > 8 else "")
                             + f": {m}")

            base, rover = self.channels["base"], self.channels["rover"]
            eph_sys = defaultdict(int)
            for ee in self.eph.values():
                eph_sys[ee["sys"]] += 1
            return {
                "ts": now,
                "status": base.status,           # header pill follows base
                "channels": chan_stats,
                "stats": {                       # aggregate (both channels)
                    "station": base.station,
                    "tow_ms": base.tow_ms if base.tow_ms is not None else rover.tow_ms,
                    "sysname": base.last_sysname,
                    "sats": agg["sats"],
                    "signals": agg["signals"],
                    "multi_freq": agg["multi_freq"],
                    "msg_rate": agg["rate"],
                    "msg_count": agg["msg_count"],
                    "bytes_in": agg["bytes_in"],
                    "frames": agg["frames"],
                },
                "systems": dict(sys_count),
                "sats": sats_out,
                "events": list(self.events),
                "eph": {
                    "state": (self.eph_source.__class__.__name__
                              if self.eph_source else None),
                    "conn": self.eph_status,
                    "status": getattr(self.eph_source, "resp", "") if self.eph_source else "",
                    "msg_count": self.eph_msg_count,
                    "bytes_in": getattr(self.eph_source, "bytes_in", 0) if self.eph_source else 0,
                    "sats": len(self.eph),
                    "by_sys": dict(eph_sys),
                    "base_known": self.base_xyz is not None,
                    "base_source": self.base_xyz_source,
                    "base_quality": self.base_xyz_quality,
                    "base_xyz": ([round(v, 4) for v in self.base_xyz]
                                 if self.base_xyz else None),
                },
            }

    def start(self, role, src):
        self.stop(role)
        ch = self.channels[role]
        with self.lock:
            ch.reset()
            if role == "base":
                self._clear_base_position_locked()
        ch.source = src
        # 连接即录: 为本通道建录制器 (挂载点名用于落盘目录, 文件回放回退到文件名)
        mount = getattr(src, "mountpoint", None) \
            or os.path.basename(getattr(src, "path", "") or "") or None
        try:
            ch.recorder = rec.Recorder(
                RECORD_DIR, self.sid, role, ch.label, mount,
                sat_snapshot_fn=lambda _r=role: self._sat_positions(_r),
                logger=self.log)
            with self.lock:
                self.log("info", f"[{ch.label}] 已开始录制 -> {ch.recorder.dir}")
        except Exception as e:
            ch.recorder = None
            with self.lock:
                self.log("warn", f"[{ch.label}] 录制启动失败: {e}")
        src.start()

    def stop(self, role=None):
        roles = [role] if role else list(self.channels)
        for r in roles:
            ch = self.channels[r]
            if ch.source:
                ch.source.stop()
                ch.source = None
                with self.lock:
                    self.log("info", f"[{ch.label}] 用户手动断开连接")
            if ch.recorder:
                ch.recorder.close()
                with self.lock:
                    self.log("info", f"[{ch.label}] 已停止录制 "
                             f"(本会话存 {ch.recorder.periods_saved} 周期)")
                ch.recorder = None
            with self.lock:
                ch.status = {"state": "idle", "msg": "未连接"}

    def is_recording(self):
        return any(ch.recorder is not None for ch in self.channels.values())

    def start_eph(self, src):
        self.stop_eph()
        with self.lock:
            self.eph = {}
            self.eph_msg_count = 0
        self.eph_source = src
        src.start()

    def stop_eph(self):
        if self.eph_source:
            self.eph_source.stop()
            self.eph_source = None


class SessionManager:
    """Per-browser/device isolation: each session id owns its own State
    (its own NTRIP/ephemeris connection, satellites, events …) so that
    opening the page on different devices no longer shares one global state."""

    IDLE_SEC = 90.0          # reclaim a session this long after its last /stream poll

    def __init__(self):
        self.lock = threading.Lock()
        self.states = {}     # sid -> State
        self.seen = {}       # sid -> last activity ts

    def get(self, sid):
        sid = sid or "default"
        with self.lock:
            st = self.states.get(sid)
            if st is None:
                st = State(sid)
                self.states[sid] = st
            self.seen[sid] = time.time()
            return st

    def touch(self, sid):
        with self.lock:
            if (sid or "default") in self.states:
                self.seen[sid or "default"] = time.time()

    def reap(self):
        """Stop sources and drop sessions whose browser has gone away."""
        while True:
            time.sleep(30.0)
            now = time.time()
            with self.lock:
                # 正在录制的会话不回收: 浏览器关页/设备休眠时也要维持 24h 连续录制
                dead = [s for s, t in self.seen.items()
                        if now - t > self.IDLE_SEC
                        and not (self.states.get(s) and self.states[s].is_recording())]
                victims = [(s, self.states.pop(s, None)) for s in dead]
                for s in dead:
                    self.seen.pop(s, None)
            for _sid, st in victims:
                if st:
                    try:
                        st.stop()
                        st.stop_eph()
                    except Exception:
                        pass


MANAGER = SessionManager()


def _sid_from_body(cfg):
    return cfg.get("sid") or request.args.get("sid")


# ----------------------------------------------------------------- routes
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/connect", methods=["POST"])
def api_connect():
    cfg = request.get_json(force=True)
    state = MANAGER.get(_sid_from_body(cfg))
    mode = cfg.get("mode", "ntrip")
    role = cfg.get("role", "base")
    if role not in state.channels:
        return {"ok": False, "error": f"未知通道: {role}"}, 400
    on_frame = lambda fr, _r=role: state.on_frame(fr, _r)
    on_status = lambda st, _r=role: state.on_status(st, _r)
    try:
        if mode == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], on_frame, on_status,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              on_frame, on_status,
                              gga=cfg.get("gga", ""))
        state.start(role, src)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    cfg = request.get_json(silent=True) or {}
    role = cfg.get("role", "base")
    state = MANAGER.get(_sid_from_body(cfg))
    state.stop(role if role in state.channels else None)
    return {"ok": True}


@app.route("/api/connect_eph", methods=["POST"])
def api_connect_eph():
    cfg = request.get_json(force=True)
    state = MANAGER.get(_sid_from_body(cfg))
    try:
        if cfg.get("mode") == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], state.on_eph_frame, lambda s: None,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              state.on_eph_frame, state.on_eph_status,
                              gga=cfg.get("gga", ""))
        state.start_eph(src)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.route("/api/disconnect_eph", methods=["POST"])
def api_disconnect_eph():
    cfg = request.get_json(silent=True) or {}
    MANAGER.get(_sid_from_body(cfg)).stop_eph()
    return {"ok": True}


@app.route("/api/recordings")
def api_recordings():
    """列出本会话已封存的录制周期 (验证 + 第三期导出概览)。"""
    sid = request.args.get("sid") or "default"
    out = {"ok": True, "dir": RECORD_DIR,
           "channels": rec.list_recordings(RECORD_DIR, sid)}
    out["range"] = exp.available_range(RECORD_DIR, sid)   # 可导出时间范围
    return out


def _export_window(cfg):
    """从请求体解析导出时间窗 -> (start_ts, end_ts) epoch 秒。

    支持: mode='last' + hours (最近 N 小时); mode='range' + start/end (epoch 秒);
    缺省回退到该会话全部已录范围。"""
    sid = cfg.get("sid") or "default"
    now = time.time()
    mode = cfg.get("mode", "last")
    if mode == "range" and cfg.get("start") and cfg.get("end"):
        return int(float(cfg["start"])), int(float(cfg["end"]))
    if mode == "last" and cfg.get("hours"):
        return int(now - float(cfg["hours"]) * 3600), int(now) + 1
    rng = exp.available_range(RECORD_DIR, sid)
    if rng:
        return int(rng["start"]), int(rng["end"])
    return int(now - 86400), int(now) + 1


@app.route("/api/track")
def api_track():
    """返回区间内各通道卫星轨迹点 (页面内叠加天空图用)。"""
    sid = request.args.get("sid") or "default"
    cfg = {"sid": sid, "mode": request.args.get("mode", "last"),
           "hours": request.args.get("hours"),
           "start": request.args.get("start"), "end": request.args.get("end")}
    s, e = _export_window(cfg)
    return {"ok": True, "start": s, "end": e,
            "channels": exp.read_track_points(RECORD_DIR, sid, s, e)}


@app.route("/api/export", methods=["POST"])
def api_export():
    """执行导出: 写 exports/<sid>/<range>/ 并打包 zip, 返回概览 + 下载链接。"""
    cfg = request.get_json(force=True) or {}
    sid = cfg.get("sid") or "default"
    try:
        s, e = _export_window(cfg)
        if e <= s:
            return {"ok": False, "error": "结束时间需晚于开始时间"}, 400
        r = exp.do_export(RECORD_DIR, EXPORT_DIR, sid, s, e)
        if r.get("empty"):
            return {"ok": False, "error": "所选时段内没有已封存的录制周期"}, 404
        r["zip_url"] = (f"/api/export_zip?sid={sid}"
                        f"&name={r['zip_name']}")
        r.pop("zip_path", None)
        return r
    except Exception as ex:
        return {"ok": False, "error": str(ex)}, 400


@app.route("/api/export_zip")
def api_export_zip():
    """下载已生成的导出 zip。"""
    sid = request.args.get("sid") or "default"
    name = os.path.basename(request.args.get("name", ""))   # 防目录穿越
    folder = os.path.join(EXPORT_DIR, rec._safe(sid))
    if not name.endswith(".zip") or not os.path.isfile(os.path.join(folder, name)):
        return {"ok": False, "error": "导出文件不存在"}, 404
    return send_from_directory(folder, name, as_attachment=True)


@app.route("/api/import_track", methods=["POST"])
def api_import_track():
    """导入离线保存的轨迹文件 (_track.csv 或导出 zip), 返回轨迹点供页面叠加。"""
    f = request.files.get("file")
    if not f:
        return {"ok": False, "error": "未收到文件"}, 400
    try:
        channels = exp.import_tracks(f.filename, f.read())
        if not channels or all(v["npoints"] == 0 for v in channels.values()):
            return {"ok": False,
                    "error": "未解析到轨迹点 (需 _track.csv 或导出的 zip)"}, 400
        return {"ok": True, "channels": channels}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}, 400


@app.route("/stream")
def stream():
    sid = request.args.get("sid")
    state = MANAGER.get(sid)

    def gen():
        while True:
            MANAGER.touch(sid)
            payload = json.dumps(state.snapshot(), ensure_ascii=False)
            yield f"data: {payload}\n\n"
            time.sleep(1.0)
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="RTCM Monitor 服务")
    ap.add_argument("--host", default="127.0.0.1",
                    help="监听地址 (默认 127.0.0.1；对外访问用 0.0.0.0)")
    ap.add_argument("--port", type=int, default=7999, help="监听端口 (默认 7999)")
    args = ap.parse_args()
    shown = "127.0.0.1" if args.host in ("0.0.0.0", "") else args.host
    print(f"RTCM Monitor  ->  http://{shown}:{args.port}")
    if args.host == "0.0.0.0":
        print("注意: 已对外监听 0.0.0.0，请用防火墙限制来源 IP。")
    threading.Thread(target=MANAGER.reap, daemon=True).start()
    app.run(host=args.host, port=args.port, threaded=True)
