"""
Per-GNSS-system frequency / signal-name tables.

RTCM MSM signal codes (as decoded by pyrtcm, e.g. "2I", "1P", "5P", "7D") are
RINEX-3 attribute codes: <band><attribute>.  The first character is the RINEX
band number, the second is the tracking-mode/component attribute.

This module maps (system, band, attribute) -> nominal carrier frequency and a
human friendly signal name, for every constellation that uses MSM messages.
"""

# Constellation that a given MSM message type carries.
#   107x GPS, 108x GLONASS, 109x Galileo, 110x SBAS,
#   111x QZSS, 112x BeiDou, 113x NavIC/IRNSS
MSM_SYS = {
    "107": ("G", "GPS"),
    "108": ("R", "GLONASS"),
    "109": ("E", "Galileo"),
    "110": ("S", "SBAS"),
    "111": ("J", "QZSS"),
    "112": ("C", "BeiDou"),
    "113": ("I", "NavIC"),
}

# Nominal centre frequency (Hz) per system per RINEX band number.
# GLONASS L1/L2 are FDMA; nominal centre is used (channel offset ignored).
BAND_FREQ = {
    "G": {1: 1575.42e6, 2: 1227.60e6, 5: 1176.45e6},
    "R": {1: 1602.000e6, 2: 1246.000e6, 3: 1202.025e6, 4: 1600.995e6, 6: 1248.060e6},
    "E": {1: 1575.42e6, 5: 1176.45e6, 7: 1207.140e6, 8: 1191.795e6, 6: 1278.75e6},
    "C": {1: 1575.42e6, 2: 1561.098e6, 5: 1176.45e6, 6: 1268.52e6, 7: 1207.140e6, 8: 1191.795e6},
    "J": {1: 1575.42e6, 2: 1227.60e6, 5: 1176.45e6, 6: 1278.75e6},
    "S": {1: 1575.42e6, 5: 1176.45e6},
    "I": {5: 1176.45e6, 9: 2492.028e6},
}

# Friendly signal-band name per system per band.
BAND_NAME = {
    "G": {1: "L1", 2: "L2", 5: "L5"},
    "R": {1: "G1", 2: "G2", 3: "G3", 4: "G1a", 6: "G2a"},
    "E": {1: "E1", 5: "E5a", 7: "E5b", 8: "E5(a+b)", 6: "E6"},
    "C": {1: "B1C", 2: "B1I", 5: "B2a", 6: "B3I", 7: "B2I/B2b", 8: "B2(a+b)"},
    "J": {1: "L1", 2: "L2", 5: "L5", 6: "L6"},
    "S": {1: "L1", 5: "L5"},
    "I": {5: "L5", 9: "S"},
}

# For BeiDou the band-7 attribute tells legacy B2I from new B2b.
BDS_B7_NAME = {"I": "B2I", "Q": "B2I", "X": "B2I",
               "D": "B2b", "P": "B2b", "Z": "B2b"}
BDS_B1_NAME = {"I": "B1I", "D": "B1C", "P": "B1C", "X": "B1C",
               "S": "B1A", "L": "B1A", "Z": "B1A"}


def sys_from_msgid(identity: str):
    """('C','BeiDou') from a message id like '1124'. None if not an MSM type."""
    return MSM_SYS.get(str(identity)[:3])


def parse_code(sysid: str, code: str):
    """
    code: RINEX attribute code such as '2I', '1P', '7D'.
    Returns dict: band, attr, freq_mhz, band_name, signal_name.
    """
    code = (code or "").strip()
    band = int(code[0]) if code and code[0].isdigit() else None
    attr = code[1] if len(code) > 1 else ""
    freq = BAND_FREQ.get(sysid, {}).get(band)
    bname = BAND_NAME.get(sysid, {}).get(band, f"B{band}")

    signal = bname
    if sysid == "C" and band == 7:
        signal = BDS_B7_NAME.get(attr, "B2?")
    elif sysid == "C" and band == 1:
        signal = BDS_B1_NAME.get(attr, "B1?")

    return {
        "band": band,
        "attr": attr,
        "freq_mhz": round(freq / 1e6, 3) if freq else None,
        "band_name": bname,
        "signal": signal,        # e.g. B1I, B2b, L1, E5a
        "code": code,            # raw RINEX code, e.g. 7D
    }
