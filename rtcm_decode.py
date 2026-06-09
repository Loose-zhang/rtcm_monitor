"""
RTCM3 framing + MSM observation decoding.

- RtcmFramer: pull complete, CRC-checked RTCM3 frames out of an arbitrary byte
  stream (works for both a local file and a raw NTRIP socket; resynchronises by
  scanning for the 0xD3 preamble and validating CRC-24Q).
- decode_msm(parsed): turn a pyrtcm-parsed MSM message into a flat list of
  per-satellite-per-signal observation cells (PRN, signal, freq, CN0, range).
"""
from pyrtcm import RTCMReader
import freq_table as ft

C_LIGHT = 299792458.0

# ---------------------------------------------------------------- CRC-24Q
_CRC_POLY = 0x1864CFB

def crc24q(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= b << 16
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= _CRC_POLY
    return crc & 0xFFFFFF


class RtcmFramer:
    """Feed bytes, get back validated raw RTCM3 frames."""
    def __init__(self):
        self.buf = bytearray()

    def feed(self, chunk: bytes):
        self.buf.extend(chunk)
        out = []
        while True:
            i = self.buf.find(0xD3)
            if i < 0:
                self.buf.clear()
                break
            if i > 0:
                del self.buf[:i]              # drop junk before preamble
            if len(self.buf) < 3:
                break
            length = ((self.buf[1] & 0x03) << 8) | self.buf[2]
            total = 3 + length + 3            # header + payload + crc24
            if len(self.buf) < total:
                break
            frame = bytes(self.buf[:total])
            calc = crc24q(frame[:-3])
            recv = (frame[-3] << 16) | (frame[-2] << 8) | frame[-1]
            if calc == recv:
                out.append(frame)
                del self.buf[:total]
            else:
                del self.buf[:1]              # bad CRC -> resync one byte on
        return out


# ----------------------------------------------------- MSM epoch time field
# Each constellation puts its epoch time in a different DF right after DF003.
_TOW_DF = {"G": "DF004", "R": "DF034", "E": "DF248",
           "S": "DF004", "J": "DF428", "C": "DF427", "I": "DF546"}


def _epoch_ms(parsed, sysid):
    for df in (_TOW_DF.get(sysid), "DF004", "DF427", "DF248", "DF428", "DF034"):
        v = getattr(parsed, df, None) if df else None
        if v is not None:
            return v
    return None


def decode_msm(parsed):
    """
    Return None for non-MSM messages, else:
      {sys, sysname, station, msg, tow_ms, cells:[...]}
    Each cell: {sat, prn, code, signal, band, freq_mhz, cn0, pseudorange, valid}
    Handles MSM4/5/6/7 (CN0 in DF403 6-bit or DF408 extended float).
    """
    ident = str(parsed.identity)
    sysinfo = ft.sys_from_msgid(ident)
    if not sysinfo:
        return None
    sysid, sysname = sysinfo

    nsat = getattr(parsed, "NSat", 0)
    ncell = getattr(parsed, "NCell", 0)
    if not ncell:
        return None

    # satellite rough range (ms): integer DF397 + modulo DF398
    satN, satR = {}, {}
    for i in range(1, nsat + 1):
        pr = getattr(parsed, f"PRN_{i:02d}", None)
        if pr is None:
            continue
        satN[pr] = getattr(parsed, f"DF397_{i:02d}", None)
        satR[pr] = getattr(parsed, f"DF398_{i:02d}", None)

    cells = []
    for i in range(1, ncell + 1):
        pr = getattr(parsed, f"CELLPRN_{i:02d}", None)
        sg = getattr(parsed, f"CELLSIG_{i:02d}", None)
        if pr is None or sg is None:
            continue
        prn = int(pr)
        meta = ft.parse_code(sysid, sg)

        # CN0: MSM4/6 -> DF403 (int dB-Hz); MSM5/7 -> DF408 (float)
        cn0 = getattr(parsed, f"DF403_{i:02d}", None)
        if cn0 is None:
            cn0 = getattr(parsed, f"DF408_{i:02d}", None)
        cn0 = round(float(cn0), 1) if cn0 is not None else None

        # pseudorange (m) if fine field present
        prange = None
        fine = getattr(parsed, f"DF400_{i:02d}", None)
        if fine is None:
            fine = getattr(parsed, f"DF405_{i:02d}", None)  # MSM5/7 extended
        rough = None
        if satN.get(pr) is not None and satR.get(pr) is not None:
            rough = satN[pr] + satR[pr]
        if rough is not None and fine is not None:
            prange = round((rough + fine) * C_LIGHT / 1000.0, 3)

        cells.append({
            "sat": f"{sysid}{prn:02d}",
            "sys": sysid,
            "prn": prn,
            "code": meta["code"],
            "signal": meta["signal"],
            "band": meta["band"],
            "band_name": meta["band_name"],
            "freq_mhz": meta["freq_mhz"],
            "cn0": cn0,
            "pseudorange": prange,
            "valid": cn0 is not None and cn0 > 0,
        })

    return {
        "sys": sysid,
        "sysname": sysname,
        "station": getattr(parsed, "DF003", None),
        "msg": ident,
        "tow_ms": _epoch_ms(parsed, sysid),
        "cells": cells,
    }


def frames_to_messages(frames):
    """Parse a list of raw frames -> pyrtcm messages (skipping unparseable)."""
    out = []
    for fr in frames:
        try:
            out.append(RTCMReader.parse(fr))
        except Exception:
            continue
    return out
