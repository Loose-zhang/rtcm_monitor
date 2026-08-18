"""
Broadcast-ephemeris -> satellite ECEF position -> azimuth/elevation.

Decodes RTCM3 ephemeris messages (1019 GPS, 1042 BeiDou, 1045/1046 Galileo,
1044 QZSS) via pyrtcm field names, runs the standard Keplerian broadcast-orbit
algorithm (with BeiDou GEO special rotation), and converts to az/el relative to
the observer ECEF (from 1005/1006 or the SPP fallback). GLONASS (1020) ephemeris is stored
but not propagated (not needed for the current BeiDou-only observation skyplot).

pyrtcm already applies each field's scale factor, so the main angles
(M0, Ω0, i0, ω) arrive in SEMICIRCLES and the rates (Δn, IDOT, ΩDOT) in
SEMICIRCLES/s — both are multiplied by π here to get radians/(radians per s).
"""
import math

PI = 3.1415926535898            # GPS/BDS ICD value of pi

# (mu, omega_earth) per system
CONST = {
    "G": (3.9860050e14, 7.2921151467e-5),
    "J": (3.9860050e14, 7.2921151467e-5),
    "E": (3.986004418e14, 7.2921151467e-5),
    "C": (3.986004418e14, 7.292115e-5),
}

# message id -> (system char, field map). Angles converted with *PI later.
FIELDS = {
    "1019": ("G", dict(sat="DF009", week="DF076", toe="DF093", M0="DF088",
                       dn="DF087", e="DF090", sqrtA="DF092", Om0="DF095",
                       i0="DF097", w="DF099", Omd="DF100", idot="DF079",
                       Cuc="DF089", Cus="DF091", Crc="DF098", Crs="DF086",
                       Cic="DF094", Cis="DF096", health="DF102",
                       toc="DF081", af2="DF082", af1="DF083", af0="DF084",
                       tgd="DF101")),
    "1042": ("C", dict(sat="DF488", week="DF489", toe="DF505", M0="DF500",
                       dn="DF499", e="DF502", sqrtA="DF504", Om0="DF507",
                       i0="DF509", w="DF511", Omd="DF512", idot="DF491",
                       Cuc="DF501", Cus="DF503", Crc="DF510", Crs="DF498",
                       Cic="DF506", Cis="DF508", health="DF515",
                       toc="DF493", af2="DF494", af1="DF495", af0="DF496",
                       tgd="DF513")),
    "1045": ("E", dict(sat="DF252", week="DF289", toe="DF304", M0="DF299",
                       dn="DF298", e="DF301", sqrtA="DF303", Om0="DF306",
                       i0="DF308", w="DF310", Omd="DF311", idot="DF292",
                       Cuc="DF300", Cus="DF302", Crc="DF309", Crs="DF297",
                       Cic="DF305", Cis="DF307", health="DF314",
                       toc="DF293", af2="DF294", af1="DF295", af0="DF296",
                       tgd="DF312")),
    "1046": ("E", dict(sat="DF252", week="DF289", toe="DF304", M0="DF299",
                       dn="DF298", e="DF301", sqrtA="DF303", Om0="DF306",
                       i0="DF308", w="DF310", Omd="DF311", idot="DF292",
                       Cuc="DF300", Cus="DF302", Crc="DF309", Crs="DF297",
                       Cic="DF305", Cis="DF307", health="DF287",
                       toc="DF293", af2="DF294", af1="DF295", af0="DF296",
                       tgd="DF313")),
    "1044": ("J", dict(sat="DF429", week="DF452", toe="DF442", M0="DF437",
                       dn="DF436", e="DF439", sqrtA="DF441", Om0="DF444",
                       i0="DF446", w="DF448", Omd="DF449", idot="DF450",
                       Cuc="DF438", Cus="DF440", Crc="DF447", Crs="DF435",
                       Cic="DF443", Cis="DF445", health="DF454",
                       toc="DF430", af2="DF431", af1="DF432", af0="DF433",
                       tgd="DF455")),
}
EPH_TYPES = set(FIELDS) | {"1020"}


def normalize(parsed):
    """RTCM eph message -> dict of orbital elements (angles in rad). None if N/A."""
    ident = str(parsed.identity)
    if ident not in FIELDS:
        return None
    sysc, fm = FIELDS[ident]
    g = lambda k: getattr(parsed, fm[k], None)
    prn = g("sat")
    if prn is None:
        return None
    prn = int(prn)
    # QZSS 1044 的 DF429 是 SVID(1-10)，而 MSM 观测里 QZSS 用 PRN(193-202)；
    # 这里 +192 对齐，否则星历键 J01 与观测键 J194 不匹配 → 永远算不出位置。
    if sysc == "J":
        prn += 192
    try:
        # BeiDou DF513/DF514 are decoded by pyrtcm in nanoseconds; the other
        # constellations' group-delay fields are already scaled in seconds.
        tgd = g("tgd")
        if sysc == "C" and tgd is not None:
            tgd *= 1e-9
        eph = {
            "sys": sysc, "prn": prn, "sat": f"{sysc}{prn:02d}",
            "week": g("week"), "toe": g("toe"),
            "M0": g("M0") * PI, "dn": g("dn") * PI, "e": g("e"),
            "sqrtA": g("sqrtA"), "Om0": g("Om0") * PI, "i0": g("i0") * PI,
            "w": g("w") * PI, "Omd": g("Omd") * PI, "idot": g("idot") * PI,
            "Cuc": g("Cuc"), "Cus": g("Cus"), "Crc": g("Crc"), "Crs": g("Crs"),
            "Cic": g("Cic"), "Cis": g("Cis"), "health": g("health"),
            "toc": g("toc"), "af0": g("af0"), "af1": g("af1"),
            "af2": g("af2"), "tgd": tgd,
        }
    except (TypeError, ValueError):
        return None
    return eph


def _is_bds_geo(prn):
    return prn <= 5 or 59 <= prn <= 63


def sat_ecef(eph, t_sow):
    """Satellite ECEF (m) at time-of-week t_sow (seconds, same scale as toe)."""
    mu, we = CONST[eph["sys"]]
    A = eph["sqrtA"] ** 2
    if A <= 0:
        return None
    n0 = math.sqrt(mu / A**3)
    tk = t_sow - eph["toe"]
    if tk > 302400:
        tk -= 604800
    elif tk < -302400:
        tk += 604800
    n = n0 + eph["dn"]
    M = eph["M0"] + n * tk
    E = M
    for _ in range(15):
        E = M + eph["e"] * math.sin(E)
    e = eph["e"]
    nu = math.atan2(math.sqrt(1 - e*e) * math.sin(E), math.cos(E) - e)
    phi = nu + eph["w"]
    s2, c2 = math.sin(2*phi), math.cos(2*phi)
    u = phi + eph["Cus"]*s2 + eph["Cuc"]*c2
    r = A*(1 - e*math.cos(E)) + eph["Crs"]*s2 + eph["Crc"]*c2
    i = eph["i0"] + eph["idot"]*tk + eph["Cis"]*s2 + eph["Cic"]*c2
    x, y = r*math.cos(u), r*math.sin(u)
    sini, cosi = math.sin(i), math.cos(i)

    if eph["sys"] == "C" and _is_bds_geo(eph["prn"]):
        O = eph["Om0"] + eph["Omd"]*tk - we*eph["toe"]
        sO, cO = math.sin(O), math.cos(O)
        xg = x*cO - y*cosi*sO
        yg = x*sO + y*cosi*cO
        zg = y*sini
        so, co = math.sin(we*tk), math.cos(we*tk)
        c5, s5 = math.cos(math.radians(5)), math.sin(math.radians(5))
        X = xg*co + yg*so*c5 + zg*so*s5
        Y = -xg*so + yg*co*c5 + zg*co*s5
        Z = -yg*s5 + zg*c5
        return (X, Y, Z)

    O = eph["Om0"] + (eph["Omd"] - we)*tk - we*eph["toe"]
    sO, cO = math.sin(O), math.cos(O)
    X = x*cO - y*cosi*sO
    Y = x*sO + y*cosi*cO
    Z = y*sini
    return (X, Y, Z)


def sat_clock(eph, t_sow):
    """Broadcast satellite clock correction in seconds at system time t_sow.

    Includes the polynomial clock terms, relativistic correction and the
    broadcast group delay appropriate to the ephemeris message.  The result is
    the satellite clock offset used in P = range + receiver_clock - c*dts.
    """
    toc = eph.get("toc")
    if toc is None:
        return 0.0
    dt = t_sow - toc
    if dt > 302400:
        dt -= 604800
    elif dt < -302400:
        dt += 604800

    # Eccentric anomaly at transmit time for the relativistic term.
    mu, _we = CONST[eph["sys"]]
    A = eph["sqrtA"] ** 2
    tk = t_sow - eph["toe"]
    if tk > 302400:
        tk -= 604800
    elif tk < -302400:
        tk += 604800
    n = math.sqrt(mu / A**3) + eph["dn"]
    M = eph["M0"] + n * tk
    E = M
    for _ in range(15):
        E = M + eph["e"] * math.sin(E)
    rel = -4.442807633e-10 * eph["e"] * eph["sqrtA"] * math.sin(E)
    return (eph.get("af0") or 0.0) + (eph.get("af1") or 0.0) * dt \
        + (eph.get("af2") or 0.0) * dt * dt + rel - (eph.get("tgd") or 0.0)


def ecef_to_geodetic(x, y, z):
    a = 6378137.0; f = 1/298.257223563; e2 = f*(2-f)
    lon = math.atan2(y, x)
    p = math.hypot(x, y); lat = math.atan2(z, p*(1-e2))
    for _ in range(8):
        N = a/math.sqrt(1-e2*math.sin(lat)**2)
        h = p/math.cos(lat) - N
        lat = math.atan2(z, p*(1-e2*N/(N+h)))
    return lat, lon


def azel(base_xyz, sat_xyz):
    """Return (az_deg 0-360, el_deg) of sat seen from base."""
    bx, by, bz = base_xyz
    dx, dy, dz = sat_xyz[0]-bx, sat_xyz[1]-by, sat_xyz[2]-bz
    lat, lon = ecef_to_geodetic(bx, by, bz)
    sl, cl = math.sin(lat), math.cos(lat)
    so, co = math.sin(lon), math.cos(lon)
    E = -so*dx + co*dy
    N = -sl*co*dx - sl*so*dy + cl*dz
    U = cl*co*dx + cl*so*dy + sl*dz
    az = math.degrees(math.atan2(E, N)) % 360
    el = math.degrees(math.atan2(U, math.hypot(E, N)))
    return round(az, 1), round(el, 1)


if __name__ == "__main__":
    # sanity: a circular BeiDou MEO orbit -> radius ~= A, plausible az/el
    A = 27906100.0
    eph = dict(sys="C", prn=24, sat="C24", week=900, toe=117000.0,
               M0=0.3, dn=0.0, e=0.0001, sqrtA=math.sqrt(A), Om0=0.5,
               i0=math.radians(55), w=0.2, Omd=-2.0e-9, idot=0.0,
               Cuc=0, Cus=0, Crc=0, Crs=0, Cic=0, Cis=0, health=0)
    p = sat_ecef(eph, 117000.0)
    r = math.sqrt(sum(c*c for c in p))
    print("MEO radius km:", round(r/1000, 1), "(expect ~27906)")
    base = (-2324872.67, 5387637.02, 2491675.21)
    print("az/el:", azel(base, p))
