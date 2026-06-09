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
from collections import defaultdict

from flask import Flask, Response, request, send_from_directory

import rtcm_decode as rd
import bds_recon as br
import gnss_orbit as orb

STALE_SEC = 15.0          # drop a signal not refreshed for this long
app = Flask(__name__, static_folder="static")


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
        self.base_xyz = None         # base ECEF from 1005, for az/el

    # ---- called from source thread, per validated frame
    def on_frame(self, frame):
        try:
            msg = rd.RTCMReader.parse(frame)
        except Exception:
            return
        ident = str(msg.identity)
        now = time.time()
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
                self.station = getattr(msg, "DF003", None)
                x = getattr(msg, "DF025", None)
                y = getattr(msg, "DF026", None)
                z = getattr(msg, "DF027", None)
                if None not in (x, y, z):
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

    def on_status(self, st):
        with self.lock:
            self.status = st

    # ---- snapshot for the browser
    def snapshot(self):
        now = time.time()
        with self.lock:
            sats_out = []
            sys_count = defaultdict(int)
            sig_total = 0
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
                    "msg_rate": rate,
                    "msg_count": self.msg_count,
                    "bytes_in": bytes_in,
                    "frames": frames,
                },
                "systems": dict(sys_count),
                "sats": sats_out,
                "eph": {
                    "state": (self.eph_source.__class__.__name__
                              if self.eph_source else None),
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
        self.source = src
        src.start()

    def stop(self):
        if self.source:
            self.source.stop()
            self.source = None
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


STATE = State()


# ----------------------------------------------------------------- routes
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/connect", methods=["POST"])
def api_connect():
    cfg = request.get_json(force=True)
    mode = cfg.get("mode", "ntrip")
    try:
        if mode == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], STATE.on_frame, STATE.on_status,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              STATE.on_frame, STATE.on_status,
                              gga=cfg.get("gga", ""))
        STATE.start(src)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    STATE.stop()
    return {"ok": True}


@app.route("/api/connect_eph", methods=["POST"])
def api_connect_eph():
    cfg = request.get_json(force=True)
    try:
        if cfg.get("mode") == "file":
            from ntrip_source import FileSource
            src = FileSource(cfg["path"], STATE.on_frame, lambda s: None,
                             speed=float(cfg.get("speed", 1.0)),
                             loop=bool(cfg.get("loop", True)))
        else:
            from ntrip_source import NtripSource
            src = NtripSource(cfg["host"], cfg["port"], cfg["mountpoint"],
                              cfg.get("user", ""), cfg.get("password", ""),
                              STATE.on_frame, lambda s: None,
                              gga=cfg.get("gga", ""))
        STATE.start_eph(src)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.route("/api/disconnect_eph", methods=["POST"])
def api_disconnect_eph():
    STATE.stop_eph()
    return {"ok": True}


@app.route("/stream")
def stream():
    def gen():
        while True:
            payload = json.dumps(STATE.snapshot(), ensure_ascii=False)
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
    app.run(host=args.host, port=args.port, threaded=True)
