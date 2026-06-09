"""
Data sources that produce raw RTCM bytes:
  - NtripSource : connect to an NTRIP caster (v1 ICY / v2 HTTP), optional VRS GGA
  - FileSource  : replay a local .rtcm file, paced to look real-time

Both run in their own thread and push validated RTCM frames into a callback.
"""
import base64
import socket
import threading
import time

from rtcm_decode import RtcmFramer


class _BaseSource(threading.Thread):
    def __init__(self, on_frame, on_status):
        super().__init__(daemon=True)
        self.on_frame = on_frame
        self.on_status = on_status
        self._stop = threading.Event()
        self.framer = RtcmFramer()
        self.bytes_in = 0
        self.frames = 0
        self.resp = ""

    def stop(self):
        self._stop.set()

    def _emit(self, chunk):
        self.bytes_in += len(chunk)
        for fr in self.framer.feed(chunk):
            self.frames += 1
            self.on_frame(fr)


def build_gga(lat, lon, h=30.0):
    """Build a minimal $GPGGA sentence from decimal lat/lon (for VRS mounts)."""
    def dm(v):
        d = int(abs(v)); m = (abs(v) - d) * 60
        return f"{d:02d}{m:09.6f}"
    t = time.gmtime()
    hms = f"{t.tm_hour:02d}{t.tm_min:02d}{t.tm_sec:02d}.00"
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    body = (f"GPGGA,{hms},{dm(lat)},{ns},{dm(lon)},{ew},"
            f"1,12,1.0,{h:.1f},M,0.0,M,,")
    c = 0
    for ch in body:
        c ^= ord(ch)
    return f"${body}*{c:02X}\r\n"


class ChunkedDecoder:
    """Stateful HTTP/1.1 chunked transfer-encoding decoder -> raw body bytes."""
    def __init__(self):
        self.buf = bytearray()
        self.remaining = 0
        self.state = "size"

    def feed(self, data):
        self.buf.extend(data)
        out = bytearray()
        while True:
            if self.state == "size":
                idx = self.buf.find(b"\r\n")
                if idx < 0:
                    break
                line = bytes(self.buf[:idx]).split(b";")[0].strip()
                del self.buf[:idx + 2]
                try:
                    self.remaining = int(line, 16)
                except ValueError:
                    self.remaining = 0
                if self.remaining == 0:
                    break                       # final chunk
                self.state = "data"
            elif self.state == "data":
                take = min(self.remaining, len(self.buf))
                if take == 0:
                    break
                out.extend(self.buf[:take])
                del self.buf[:take]
                self.remaining -= take
                if self.remaining == 0:
                    self.state = "trailer"
            else:                               # consume \r\n after chunk
                if len(self.buf) < 2:
                    break
                del self.buf[:2]
                self.state = "size"
        return bytes(out)


class NtripSource(_BaseSource):
    def __init__(self, host, port, mountpoint, user, password,
                 on_frame, on_status, gga=""):
        super().__init__(on_frame, on_status)
        self.host = host.strip()
        self.port = int(port)
        self.mountpoint = mountpoint.strip().lstrip("/")
        self.user = user or ""
        self.password = password or ""
        self.gga = (gga or "").strip()

    def _request(self):
        auth = base64.b64encode(f"{self.user}:{self.password}".encode()).decode()
        return (
            f"GET /{self.mountpoint} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            f"Ntrip-Version: Ntrip/2.0\r\n"
            f"User-Agent: NTRIP rtcm-monitor/1.0\r\n"
            f"Authorization: Basic {auth}\r\n"
            f"Accept: */*\r\n"
            f"Connection: close\r\n\r\n"
        ).encode()

    def run(self):
        try:
            self.on_status({"state": "connecting",
                            "msg": f"连接 {self.host}:{self.port}/{self.mountpoint}"})
            sock = socket.create_connection((self.host, self.port), timeout=10)
            sock.sendall(self._request())
            sock.settimeout(20)

            # ---- read response status line (handle ICY v1 + HTTP v2) ----
            buf = b""
            while b"\r\n" not in buf:
                ch = sock.recv(1024)
                if not ch:
                    raise ConnectionError("caster 无响应")
                buf += ch
                if len(buf) > 4096:
                    break
            status = buf.split(b"\r\n", 1)[0].decode(errors="replace").strip()
            self.resp = status
            print(f"[NTRIP] <= {status}")
            up = buf[:512].decode(errors="replace").upper()
            if "SOURCETABLE" in up:
                raise ConnectionError(f"挂载点不存在(返回 SOURCETABLE): {status}")
            if "200" not in status:
                raise ConnectionError(f"caster 拒绝: {status}")

            chunked = False
            if status.upper().startswith("HTTP"):
                while b"\r\n\r\n" not in buf:               # v2: full header block
                    ch = sock.recv(1024)
                    if not ch:
                        raise ConnectionError("caster 断开(读头中)")
                    buf += ch
                    if len(buf) > 16384:
                        break
                hdr = buf.split(b"\r\n\r\n", 1)[0].decode(errors="replace").lower()
                chunked = ("transfer-encoding" in hdr and "chunked" in hdr)
                data = buf.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in buf else b""
            else:                                            # v1 ICY: data after status line
                data = buf.split(b"\r\n", 1)[1]
            dec = ChunkedDecoder() if chunked else None
            print(f"[NTRIP] chunked={chunked}")

            def push(raw):
                payload = dec.feed(raw) if dec else raw
                if payload:
                    self._emit(payload)

            # VRS mounts need an NMEA GGA before they stream
            if self.gga:
                sock.sendall((self.gga + "\r\n").encode() if not self.gga.endswith("\n")
                             else self.gga.encode())
                print("[NTRIP] => sent GGA")

            if data:
                push(data)
            self.on_status({"state": "streaming",
                            "msg": f"已连接 {self.mountpoint}"})

            last_gga = time.time()
            last_log = 0
            while not self._stop.is_set():
                try:
                    ch = sock.recv(4096)
                except socket.timeout:
                    self.on_status({"state": "streaming",
                                    "msg": f"{self.mountpoint} · 等待数据… "
                                           f"已收 {self.bytes_in}B / {self.frames}帧"})
                    if self.gga:                              # keep VRS alive
                        try:
                            sock.sendall((self.gga + "\r\n").encode())
                        except Exception:
                            pass
                    continue
                if not ch:
                    raise ConnectionError("caster 断开连接")
                push(ch)
                now = time.time()
                if self.gga and now - last_gga > 10:
                    try:
                        sock.sendall((self.gga + "\r\n").encode())
                    except Exception:
                        pass
                    last_gga = now
                if now - last_log > 3:
                    print(f"[NTRIP] bytes={self.bytes_in} frames={self.frames}")
                    last_log = now
            sock.close()
            self.on_status({"state": "stopped", "msg": "已停止"})
        except Exception as e:
            print(f"[NTRIP] error: {e}")
            self.on_status({"state": "error", "msg": f"NTRIP 错误: {e}"})


class FileSource(_BaseSource):
    """Replay a .rtcm file. speed=0 -> as fast as possible; else ~ realtime*speed."""
    def __init__(self, path, on_frame, on_status, speed=1.0, loop=True):
        super().__init__(on_frame, on_status)
        self.path = path
        self.speed = float(speed)
        self.loop = loop

    def run(self):
        try:
            self.on_status({"state": "streaming", "msg": f"回放文件 {self.path}"})
            while not self._stop.is_set():
                with open(self.path, "rb") as fh:
                    data = fh.read()
                step = 1024
                for i in range(0, len(data), step):
                    if self._stop.is_set():
                        break
                    self._emit(data[i:i + step])
                    if self.speed > 0:
                        time.sleep(0.05 / self.speed)
                if not self.loop:
                    break
                time.sleep(0.5)
            self.on_status({"state": "stopped", "msg": "回放结束"})
        except Exception as e:
            self.on_status({"state": "error", "msg": f"文件回放错误: {e}"})
