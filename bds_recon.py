"""
BeiDou 2026/04 PRN reconstruction awareness.

After the April 2026 on-orbit reconstruction:
  * PRN 9 and 10 are the only remaining BDS-2 satellites
    (broadcasting legacy B1I / B2I / B3I).
  * All other active low PRNs (1-4, 6-8, 11-14) were taken over by BDS-3
    satellites broadcasting B1I / B3I / B1C / B2a / B2b (NO B2I).
  * PRN 5, 15, 16, 17, 18 are reserved / empty.
  * PRN 19-46 remain BDS-3 (B1I / B3I / B1C / B2a / B2b).

This lets the monitor flag two real-world receiver problems:
  1. A BDS-3 satellite on a reconstructed low PRN that only outputs B1I
     -> receiver firmware still treats the PRN as legacy BDS-2 and never
        acquires the new B1C/B2a/B2b signals.
  2. A '7I' (B2I) observation on a BDS-3 satellite -> almost certainly a
     B2b signal mislabelled as legacy B2I by an outdated receiver table.
"""

RESERVED = {5, 15, 16, 17, 18}
BDS2 = {9, 10}                       # only remaining BDS-2 satellites

# Signal "families" derived from a RINEX code (band + attribute group).
def code_family(code: str) -> str:
    code = (code or "").strip()
    if len(code) < 2:
        return "?"
    b, a = code[0], code[1]
    table = {
        "2I": "B1I", "2Q": "B1I", "2X": "B1I",
        "6I": "B3I", "6Q": "B3I", "6X": "B3I",
        "7I": "B2I", "7Q": "B2I", "7X": "B2I",
        "7D": "B2b", "7P": "B2b", "7Z": "B2b",
        "5D": "B2a", "5P": "B2a", "5X": "B2a",
        "1D": "B1C", "1P": "B1C", "1X": "B1C",
        "1S": "B1A", "1L": "B1A", "1Z": "B1A",
        "8D": "B2ab", "8P": "B2ab", "8X": "B2ab",
        "6D": "B3A", "6P": "B3A", "6Z": "B3A",
    }
    return table.get(code, f"{b}{a}")


def classify(prn: int) -> dict:
    """Return system/orbit/expected info for a BeiDou PRN (post-reconstruction)."""
    if prn in RESERVED:
        return {"sys": "reserved", "orbit": "-", "expected": set(),
                "note": "2026/4 计划中预留/空号"}
    if prn in BDS2:
        return {"sys": "BDS-2", "orbit": "IGSO",
                "expected": {"B1I", "B2I", "B3I"},
                "note": "重构后仅存的 BDS-2 卫星"}
    if 1 <= prn <= 4:
        orbit = "GEO"
    elif 6 <= prn <= 8:
        orbit = "IGSO"
    elif 11 <= prn <= 14:
        orbit = "MEO"
    elif 19 <= prn <= 46:
        orbit = "MEO/IGSO"
    elif 59 <= prn <= 63:
        orbit = "GEO"
    else:
        return {"sys": "unknown", "orbit": "?", "expected": set(),
                "note": "未知 PRN"}
    return {"sys": "BDS-3", "orbit": orbit,
            "expected": {"B1I", "B3I", "B1C", "B2a", "B2b"},
            "note": ""}


# New-signal families that distinguish a real BDS-3 from legacy-only tracking.
_NEW_BDS3 = {"B1C", "B2a", "B2b", "B2ab", "B3A"}


def check(prn: int, observed_codes) -> dict:
    """
    observed_codes: iterable of RINEX codes seen for this PRN (e.g. {'2I','7D'}).
    Returns {'sys','orbit','families','anomalies':[{level,msg}], 'ok':bool}.
    Levels: 'error' (red), 'warn' (amber), 'info' (grey).
    """
    info = classify(prn)
    fams = {code_family(c) for c in observed_codes}
    anomalies = []

    if info["sys"] == "reserved" and observed_codes:
        anomalies.append({"level": "warn",
                          "msg": "该 PRN 在 2026/4 计划中为预留/空号，却在播发，请核对"})

    elif info["sys"] == "BDS-3":
        if "B2I" in fams:
            anomalies.append({"level": "error",
                              "msg": "BDS-3 星上出现 B2I(7I)：极可能是 B2b 被旧固件误标为 legacy B2I"})
        if observed_codes and not (fams & _NEW_BDS3):
            anomalies.append({"level": "warn",
                              "msg": "BDS-3 星只锁到 legacy 信号，未采集 B1C/B2a/B2b（建议升级接收机固件）"})
        else:
            for need in ("B1C", "B2a", "B2b"):
                if observed_codes and need not in fams:
                    anomalies.append({"level": "info", "msg": f"缺少 {need}"})

    elif info["sys"] == "BDS-2":
        extra = fams & _NEW_BDS3
        if extra:
            anomalies.append({"level": "warn",
                              "msg": f"BDS-2 星出现新信号 {sorted(extra)}（异常，请核对）"})

    return {
        "sys": info["sys"],
        "orbit": info["orbit"],
        "note": info["note"],
        "families": sorted(fams),
        "anomalies": anomalies,
        "ok": len(anomalies) == 0,
    }
