"""
RTCM Monitor — web dashboard backend.

Connects to an NTRIP caster (or replays a local .rtcm file), decodes RTCM3 MSM
observations, aggregates per-satellite / per-signal CN0 + frequency, applies the
BeiDou 2026/04 reconstruction-aware checks, and streams it all to the browser
dashboard over Server-Sent Events.

Run:  python app.py   ->  open http://127.0.0.1:8765
"""
import argparse
import json
import threading
import time
from collections import defaultdict, deque

from flask import Flask, Response, request, send_from_directory

import rtcm_decode as rd
import bds_recon as br
import gnss_orbit as orb

STALE_SEC = 15.0          # drop a signal not refreshed for this long
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


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.source = None
        self.status = {"state": "idle", "msg": "未连接"}
        # sat -> {sys,prn, signals:{code:{...,'ts':t}}}
        self.sats = defaultdict(lambda: {"signals": {}})
        self.station = None
        self.tow_ms = None
        self.last_sysname = None
        self.msg_count = 0
        self.msg_times = []          # timestamps for rate calc
        self.bytes_ref = 0
        # broadcast ephemeris (separate stream)
        self.eph_source = None
        self.eph = {}                # sat -> normalized eph (+ 'ts')
        self.eph_msg_count = 0
        self.eph_status = {"state": "idle", "msg": "未连接"}
        self.base_xyz = None         # base ECEF from 1005, for az/el
        # ---- event log (ring buffer, sent to browser) ----
        self.events = deque(maxlen=200)
        self.seen_types = set()      # RTCM idents already announced
        self._active_sats = set()    # for appear/lost detection
        self._recon_flagged = {}     # sat -> set(anomaly msgs already logged)
        self._last_tick = 0.0

    def log(self, level, msg):
        """level: info / success / warn / error"""
        self.events.append({"ts": time.time(), "level": level, "msg": msg})

    # ---- called from source thread, per validated frame
    def on_frame(self, frame):
        try:
            msg = rd.RTCMReader.parse(frame)
        except Exception:
            return
        ident = str(msg.identity)
        now = time.time()
        with self.lock:
            if ident not in self.seen_types:
                self.seen_types.add(ident)
                self.log("info", f"首次收到 {ident} 报文 ({_msg_name(ident)})")
        if ident in orb.EPH_TYPES:                 # ephemeris stream
            e = orb.normalize(msg)
            with self.lock:
                self.eph_msg_count += 1
                if e:
                    e["ts"] = now
                    self.eph[e["sat"]] = e
            return
        d = rd.decode_msm(msg)
        with self.lock:
            self.msg_count += 1
            self.msg_times.append(now)
            self.msg_times = self.msg_times[-200:]
            if ident == "1005":   # base station ARP
                sid = getattr(msg, "DF003", None)
                if sid is not None and sid != self.station:
                    self.log("info", f"基准站 ID: {sid}")
                self.station = sid
                x = getattr(msg, "DF025", None)
                y = getattr(msg, "DF026", None)
                z = getattr(msg, "DF027", None)
                if None not in (x, y, z):
                    if self.base_xyz is None:
                        self.log("success", "已获取基准站 ECEF 坐标，天空图可用真实方位/高度角")
                    self.base_xyz = (x, y, z)
            if not d:
                return
            self.station = d["station"]
            self.tow_ms = d["tow_ms"]
            self.last_sysname = d["sysname"]
            for c in d["cells"]:
                s = self.sats[c["sat"]]
                s["sys"] = c["sys"]
                s["prn"] = c["prn"]
                s["signals"][c["code"]] = {
                    "code": c["code"], "signal": c["signal"],
                    "band": c["band"], "band_name": c["band_name"],
                    "freq_mhz": c["freq_mhz"], "cn0": c["cn0"],
                    "pseudorange": c["pseudorange"], "ts": now,
                }

    _ST_LEVEL = {"connecting": "info", "streaming": "success",
                 "reconnecting": "warn", "error": "error",
                 "stopped": "info", "idle": "info"}

    def on_status(self, st):
        with self.lock:
            if st.get("state") != self.status.get("state"):
                self.log(self._ST_LEVEL.get(st.get("state"), "info"),
                         st.get("msg", st.get("state", "")))
            self.status = st

    def on_eph_status(self, st):
        with self.lock:
            if st.get("state") != self.eph_status.get("state"):
                self.log(self._ST_LEVEL.get(st.get("state"), "info"),
                         f"[星历] {st.get('msg', st.get('state', ''))}")
            self.eph_status = st

    # ---- snapshot for the browser
    def snapshot(self):
        now = time.time()
        with self.lock:
            sats_out = []
            sys_count = defaultdict(int)
            sig_total = 0
            multi_freq = 0
            for sat, s in sorted(self.sats.items()):
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
                if e and self.base_xyz and self.tow_ms is not None \
                        and s.get("sys") == e["sys"]:
                    try:
                        p = orb.sat_ecef(e, self.tow_ms / 1000.0)
                        if p:
                            az, el = orb.azel(self.base_xyz, p)
                    except Exception:
                        pass
                sats_out.append({
                    "sat": sat, "sys": s.get("sys"), "prn": s.get("prn"),
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

            # ---- 1 Hz event tick (guarded so multiple SSE clients don't dup)
            if now - self._last_tick >= 0.9:
                self._last_tick = now
                cur = {so["sat"] for so in sats_out}
                gained = sorted(cur - self._active_sats)
                lost = sorted(self._active_sats - cur)
                if gained:
                    self.log("info", f"新增卫星 {', '.join(gained[:8])}"
                             + (f" 等{len(gained)}颗" if len(gained) > 8 else ""))
                if lost:
                    self.log("warn", f"失锁卫星 {', '.join(lost[:8])}"
                             + (f" 等{len(lost)}颗" if len(lost) > 8 else ""))
                    for s_ in lost:
                        self._recon_flagged.pop(s_, None)
                self._active_sats = cur
                agg = {}                      # BDS 重构异常，每星每条只记一次，同类合并
                for so in sats_out:
                    rc = so.get("recon")
                    if not rc or rc.get("ok"):
                        continue
                    flagged = self._recon_flagged.setdefault(so["sat"], set())
                    for a in rc["anomalies"]:
                        if a["level"] == "info" or a["msg"] in flagged:
                            continue
                        flagged.add(a["msg"])
                        lv, lst = agg.setdefault(a["msg"], (a["level"], []))
                        lst.append(so["sat"])
                for m, (lv, lst) in agg.items():
                    self.log(lv, f"{', '.join(lst[:8])}"
                             + (f" 等{len(lst)}颗" if len(lst) > 8 else "")
                             + f": {m}")

            times = [t for t in self.msg_times if now - t <= 5]
            rate = round(len(times) / 5.0, 1)
            bytes_in = getattr(self.source, "bytes_in", 0) if self.source else 0
            frames = getattr(self.source, "frames", 0) if self.source else 0
            eph_sys = defaultdict(int)
            for ee in self.eph.values():
                eph_sys[ee["sys"]] += 1
            return {
                "ts": now,
                "status": self.status,
                "stats": {
                    "station": self.station,
                    "tow_ms": self.tow_ms,
                    "sysname": self.last_sysname,
                    "sats": len(sats_out),
                    "signals": sig_total,
                    "multi_freq": multi_freq,
                    "msg_rate": rate,
                    "msg_count": self.msg_count,
                    "bytes_in": bytes_in,
                    "frames": frames,
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
                },
            }

    def start(self, src):
        self.stop()
        with self.lock:
            self.sats = defaultdict(lambda: {"signals": {}})
            self.msg_count = 0
            self.msg_times = []
            self.station = None
            self.tow_ms = None
            self.last_sysname = None
            self.seen_types = set()
            self._active_sats = set()
            self._recon_flagged = {}
        self.source = src
        src.start()

    def stop(self):
        if self.source:
            self.source.stop()
            self.source = None
            with self.lock:
                self.log("info", "用户手动断开连接")
        with self.lock:
            self.status = {"state": "idle", "msg": "未连接"}

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
                st = State()
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
                dead = [s for s, t in self.seen.items()
                        if now - t > self.IDLE_SEC]
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
    try:
        if mode == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], state.on_frame, state.on_status,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              state.on_frame, state.on_status,
                              gga=cfg.get("gga", ""))
        state.start(src)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    cfg = request.get_json(silent=True) or {}
    MANAGER.get(_sid_from_body(cfg)).stop()
    return {"ok": True}


@app.route("/api/connect_eph", methods=["POST"])
def api_connect_eph():
    cfg = request.get_json(force=True)
    state = MANAGER.get(_sid_from_body(cfg))
    try:
        if cfg.get("mode") == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], state.on_frame, lambda s: None,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              state.on_frame, state.on_eph_status,
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
    ap.add_argument("--port", type=int, default=8765, help="监听端口 (默认 8765)")
    args = ap.parse_args()
    shown = "127.0.0.1" if args.host in ("0.0.0.0", "") else args.host
    print(f"RTCM Monitor  ->  http://{shown}:{args.port}")
    if args.host == "0.0.0.0":
        print("注意: 已对外监听 0.0.0.0，请用防火墙限制来源 IP。")
    threading.Thread(target=MANAGER.reap, daemon=True).start()
    app.run(host=args.host, port=args.port, threaded=True)
