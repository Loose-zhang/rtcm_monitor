"""Coarse single-point positioning from RTCM MSM pseudoranges.

This is intentionally a fallback for sky-plot observer coordinates when a
filtered NTRIP stream omits RTCM 1005/1006.  It solves one constellation at a
time, avoiding inter-system clock-bias estimation.  Broadcast satellite clock
and Earth-rotation corrections are included; ionosphere/troposphere are not,
so the result is meter-level and must not be treated as a surveyed base point.
"""
import math

import gnss_orbit as orbit

C_LIGHT = 299792458.0
MIN_SATS = 5
MAX_EPH_AGE = 21600.0


def _wrap_week(dt):
    if dt > 302400:
        return dt - 604800
    if dt < -302400:
        return dt + 604800
    return dt


def _ecef_height(xyz):
    """Approximate WGS84 ellipsoidal height for rejecting non-surface roots."""
    x, y, z = xyz
    a = 6378137.0
    f = 1 / 298.257223563
    e2 = f * (2 - f)
    p = math.hypot(x, y)
    lat = math.atan2(z, p * (1 - e2))
    for _ in range(8):
        sin_lat = math.sin(lat)
        n = a / math.sqrt(1 - e2 * sin_lat * sin_lat)
        lat = math.atan2(z + e2 * n * sin_lat, p)
    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    n = a / math.sqrt(1 - e2 * sin_lat * sin_lat)
    if abs(cos_lat) > 1e-8:
        return p / cos_lat - n
    return abs(z) / max(abs(sin_lat), 1e-8) - n * (1 - e2)


def _solve_linear(a, b):
    """Solve a small dense linear system with pivoted Gaussian elimination."""
    n = len(b)
    m = [list(map(float, a[i])) + [float(b[i])] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[pivot][col]) < 1e-12:
            return None
        m[col], m[pivot] = m[pivot], m[col]
        div = m[col][col]
        for j in range(col, n + 1):
            m[col][j] /= div
        for r in range(n):
            if r == col:
                continue
            f = m[r][col]
            for j in range(col, n + 1):
                m[r][j] -= f * m[col][j]
    return [m[i][n] for i in range(n)]


def _least_squares(rows, residuals, weights):
    normal = [[0.0] * 4 for _ in range(4)]
    rhs = [0.0] * 4
    for h, v, w in zip(rows, residuals, weights):
        for i in range(4):
            rhs[i] += w * h[i] * v
            for j in range(4):
                normal[i][j] += w * h[i] * h[j]
    return _solve_linear(normal, rhs)


def _satellite_state(eph, receive_tow, pseudorange):
    """Satellite ECEF and clock at approximate signal transmit time."""
    travel = pseudorange / C_LIGHT
    tx = receive_tow - travel
    xyz = orbit.sat_ecef(eph, tx)
    if xyz is None:
        return None
    # Rotate transmit-time ECEF into the receive-time ECEF frame (Sagnac).
    we = orbit.CONST[eph["sys"]][1]
    ang = we * travel
    ca, sa = math.cos(ang), math.sin(ang)
    x, y, z = xyz
    rotated = (ca * x + sa * y, -sa * x + ca * y, z)
    return rotated, orbit.sat_clock(eph, tx)


def _iterate(observations, eph_by_sat, tow_s, active, initial_xyz=None):
    x, y, z = initial_xyz or (0.0, 0.0, 0.0)
    clock_m = 0.0
    used = []
    for _ in range(12):
        rows, vals, weights, used = [], [], [], []
        for idx in active:
            obs = observations[idx]
            eph = eph_by_sat.get(obs["sat"])
            state = _satellite_state(eph, tow_s, obs["pseudorange"])
            if state is None:
                continue
            (sx, sy, sz), sat_clk = state
            dx, dy, dz = x - sx, y - sy, z - sz
            rho = math.sqrt(dx * dx + dy * dy + dz * dz)
            if rho <= 0:
                continue
            predicted = rho + clock_m - C_LIGHT * sat_clk
            rows.append([dx / rho, dy / rho, dz / rho, 1.0])
            vals.append(obs["pseudorange"] - predicted)
            cn0 = obs.get("cn0") or 30.0
            weights.append(max(0.2, min(2.0, 10 ** ((cn0 - 40.0) / 20.0))))
            used.append(idx)
        if len(rows) < 4:
            return None
        step = _least_squares(rows, vals, weights)
        if step is None:
            return None
        x += step[0]; y += step[1]; z += step[2]; clock_m += step[3]
        if math.sqrt(step[0]**2 + step[1]**2 + step[2]**2) < 0.01:
            break

    residuals = []
    for idx in used:
        obs = observations[idx]
        state = _satellite_state(eph_by_sat[obs["sat"]], tow_s,
                                 obs["pseudorange"])
        (sx, sy, sz), sat_clk = state
        rho = math.sqrt((x-sx)**2 + (y-sy)**2 + (z-sz)**2)
        residuals.append((idx, obs["pseudorange"]
                          - (rho + clock_m - C_LIGHT * sat_clk)))
    return (x, y, z), clock_m, residuals


def solve(observations, eph_by_sat, tow_ms, system, initial_xyz=None):
    """Return a validated SPP solution dict, or None.

    observations: [{sat, pseudorange, cn0}], one pseudorange per satellite.
    eph_by_sat: normalized broadcast ephemerides keyed by satellite name.
    """
    if tow_ms is None or system not in orbit.CONST:
        return None
    tow_s = tow_ms / 1000.0
    usable = []
    for obs in observations:
        pr = obs.get("pseudorange")
        eph = eph_by_sat.get(obs.get("sat"))
        if not pr or not (1e6 < pr < 6e7) or not eph or eph.get("sys") != system:
            continue
        if eph.get("health") not in (None, 0):
            continue
        if abs(_wrap_week(tow_s - eph["toe"])) > MAX_EPH_AGE:
            continue
        usable.append(obs)
    if len(usable) < MIN_SATS:
        return None

    active = list(range(len(usable)))
    result = None
    # Robustly discard one gross pseudorange outlier at a time.
    while len(active) >= MIN_SATS:
        result = _iterate(usable, eph_by_sat, tow_s, active, initial_xyz)
        if result is None:
            return None
        xyz, clock_m, residuals = result
        worst = max(residuals, key=lambda p: abs(p[1]))
        if abs(worst[1]) <= 300.0 or len(active) == MIN_SATS:
            break
        active.remove(worst[0])

    xyz, clock_m, residuals = result
    radius = math.sqrt(sum(v * v for v in xyz))
    if not 6.15e6 < radius < 6.65e6:
        return None
    height = _ecef_height(xyz)
    if not -1500.0 < height < 50000.0:
        return None
    rms = math.sqrt(sum(v * v for _i, v in residuals) / len(residuals))
    max_res = max(abs(v) for _i, v in residuals)
    if rms > 300.0 or max_res > 1000.0:
        return None
    return {
        "xyz": xyz, "clock_m": clock_m, "system": system,
        "tow_ms": tow_ms, "nsat": len(residuals),
        "height_m": round(height, 1),
        "rms_m": round(rms, 2), "max_residual_m": round(max_res, 2),
        "sats": [usable[i]["sat"] for i, _v in residuals],
    }
