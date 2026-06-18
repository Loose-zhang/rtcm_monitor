"""
RTCM Monitor — 导出与轨迹叠加 (第三期 / 项4)。

在第二期录制底座 (``recorder.py``) 落盘的基础上，按用户选择的时间段 (绝对起止
或"最近 N 小时") 把每个通道 (base/rover) 的录制取出，产出三类文件，并**按挂载点
名称分文件夹**保存：

    <挂载点>/
        <range>.rtcm        ← 区间内各周期"最后完整历元"原始报文按时序合并(可回放)
        <range>_track.csv    ← 区间内所有周期卫星位置快照合并 (time,sat,sys,az,el,cn0)
        <range>_skyplot.html ← 该通道 24h 轨迹叠加天空图 (自包含 SVG, 浏览器直接打开)

两种交付方式同时提供：
  * 写到服务端 ``exports/<sid>/<range>/`` 目录长期归档；
  * 打包成 ``<range>.zip`` 供浏览器下载。

本模块只读录制目录、不依赖 pyrtcm / matplotlib，纯标准库实现。
"""
import calendar
import io
import json
import math
import os
import time
import zipfile

import recorder as rec   # 复用 _safe / _stamp / list_recordings


# --------------------------------------------------------------------------- helpers
def _parse_iso(s):
    """'2026-06-18T04:00:32Z' -> epoch 秒, 失败返回 None。"""
    try:
        return calendar.timegm(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        return None


def _range_label(start_ts, end_ts):
    a = time.strftime("%Y%m%dT%H%MZ", time.gmtime(start_ts))
    b = time.strftime("%Y%m%dT%H%MZ", time.gmtime(end_ts))
    return f"{a}__{b}"


def _channel_dirs(base_dir, sid):
    """返回 [(dirname, abs_dir, index_items[]), ...]，按目录名排序。"""
    root = os.path.join(base_dir, rec._safe(sid))
    out = []
    if not os.path.isdir(root):
        return out
    for d in sorted(os.listdir(root)):
        cdir = os.path.join(root, d)
        idx = os.path.join(cdir, "index.jsonl")
        if not os.path.isfile(idx):
            continue
        items = []
        with open(idx, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    items.append(json.loads(ln))
                except Exception:
                    pass
        out.append((d, cdir, items))
    return out


def available_range(base_dir, sid):
    """本会话所有通道已封存周期覆盖的 [min_start, max_start] (epoch)，无则 None。"""
    starts = []
    for _d, _cdir, items in _channel_dirs(base_dir, sid):
        for it in items:
            if it.get("period_start") is not None:
                starts.append(it["period_start"])
    if not starts:
        return None
    return {"start": min(starts), "end": max(starts) + rec.PERIOD_SEC,
            "periods": len(starts)}


def _filter_periods(items, start_ts, end_ts):
    """取 period_start 落在 [start_ts, end_ts) 的周期, 去重 + 时序排序。"""
    seen, out = set(), []
    for it in items:
        ps = it.get("period_start")
        if ps is None or ps in seen:
            continue
        if start_ts is not None and ps < start_ts:
            continue
        if end_ts is not None and ps >= end_ts:
            continue
        seen.add(ps)
        out.append(it)
    out.sort(key=lambda x: x["period_start"])
    return out


# --------------------------------------------------------------------------- track points
def read_track_points(base_dir, sid, start_ts, end_ts):
    """读区间内所有通道的卫星位置点, 供页面内轨迹叠加。

    返回 {dirname: {role, mount, npoints, points:[[t,sat,sys,az,el,cn0], ...]}}。
    点按 (sat,时间) 排序, 便于前端连成轨迹线。
    """
    res = {}
    for d, cdir, items in _channel_dirs(base_dir, sid):
        periods = _filter_periods(items, start_ts, end_ts)
        if not periods:
            continue
        role = items[0].get("role") if items else None
        mount = items[0].get("mount") if items else None
        pts = []
        for p in periods:
            csv = os.path.join(cdir, p["stamp"] + ".csv")
            if not os.path.isfile(csv):
                continue
            with open(csv, encoding="utf-8") as fh:
                next(fh, None)                       # 跳表头
                for ln in fh:
                    f = ln.rstrip("\n").split(",")
                    if len(f) < 7:
                        continue
                    t, sat, sysc, _prn, az, el, cn0 = f[:7]
                    if not az or not el:
                        continue
                    try:
                        pts.append([_parse_iso(t) or 0, sat, sysc,
                                    float(az), float(el),
                                    float(cn0) if cn0 else None])
                    except Exception:
                        pass
        pts.sort(key=lambda r: (r[1], r[0]))         # sat, then time
        res[d] = {"role": role, "mount": mount, "npoints": len(pts),
                  "points": pts}
    return res


# --------------------------------------------------------------------------- merged artifacts
def _merge_rtcm(cdir, periods):
    buf = bytearray()
    for p in periods:
        fp = os.path.join(cdir, p["stamp"] + ".rtcm")
        if os.path.isfile(fp):
            with open(fp, "rb") as fh:
                buf += fh.read()
    return bytes(buf)


def _merge_track_csv(cdir, periods):
    rows = ["time_utc,sat,sys,prn,az_deg,el_deg,cn0"]
    for p in periods:
        fp = os.path.join(cdir, p["stamp"] + ".csv")
        if not os.path.isfile(fp):
            continue
        with open(fp, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        rows.extend(ln for ln in lines[1:] if ln.strip())
    return ("\n".join(rows) + "\n").encode("utf-8")


# --------------------------------------------------------------------------- skyplot HTML (self-contained SVG)
_SYS_COLOR = {"C": "#f85149", "G": "#4aa8ff", "E": "#3fb950", "R": "#ffa657",
              "J": "#bc8cff", "S": "#8b9bb0", "I": "#d2a8ff"}


def _proj(az, el, cx, cy, R):
    r = R * (90.0 - el) / 90.0
    a = math.radians(az)
    return cx + r * math.sin(a), cy - r * math.cos(a)


def build_skyplot_html(title, points, role=None):
    """用区间内的卫星位置点生成自包含轨迹叠加天空图 (静态 SVG + HTML)。

    每颗卫星 24h 的位置点按时间连成轨迹折线, 颜色按星座; 末点画标记并标星号,
    base 用圆点、rover 用十字。"""
    cx = cy = 320
    R = 300
    # group by sat
    bysat = {}
    for t, sat, sysc, az, el, cn0 in points:
        if el is None or el < 0:
            continue
        bysat.setdefault(sat, {"sys": sysc, "pts": []})["pts"].append((t, az, el, cn0))
    svg = []
    # background rings + grid (10°/30°/60°/75° + N/E/S/W)
    svg.append(f'<circle cx="{cx}" cy="{cy}" r="{R}" fill="#0f1419" stroke="#2c3a48"/>')
    for el_ring, col in ((30, "#2c3a48"), (60, "#2c3a48")):
        rr = R * (90 - el_ring) / 90
        svg.append(f'<circle cx="{cx}" cy="{cy}" r="{rr:.1f}" fill="none" stroke="{col}"/>')
    for el_ring in (10, 75):
        rr = R * (90 - el_ring) / 90
        svg.append(f'<circle cx="{cx}" cy="{cy}" r="{rr:.1f}" fill="none" '
                   f'stroke="#f0a020" stroke-width="1.5" stroke-dasharray="6 4"/>')
    svg.append(f'<line x1="{cx}" y1="{cy-R}" x2="{cx}" y2="{cy+R}" stroke="#2c3a48"/>')
    svg.append(f'<line x1="{cx-R}" y1="{cy}" x2="{cx+R}" y2="{cy}" stroke="#2c3a48"/>')
    for txt, x, y in (("N", cx, cy-R+14), ("E", cx+R-10, cy+4),
                      ("S", cx, cy+R-6), ("W", cx-R+10, cy+4)):
        svg.append(f'<text x="{x}" y="{y}" fill="#8b9bb0" font-size="14" '
                   f'text-anchor="middle">{txt}</text>')
    # trajectories
    labels = []
    for sat, info in sorted(bysat.items()):
        color = _SYS_COLOR.get(info["sys"], "#8b9bb0")
        pts = info["pts"]
        xy = [_proj(az, el, cx, cy, R) for (_t, az, el, _c) in pts]
        if len(xy) > 1:
            d = " ".join(("M" if i == 0 else "L") + f"{x:.1f} {y:.1f}"
                         for i, (x, y) in enumerate(xy))
            dash = ' stroke-dasharray="4 3"' if role == "rover" else ""
            svg.append(f'<path d="{d}" fill="none" stroke="{color}" '
                       f'stroke-width="1.6" opacity="0.85"{dash}><title>{sat}</title></path>')
        # mark every sampled point lightly, last point emphasized
        for x, y in xy:
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="1.6" fill="{color}" opacity="0.55"/>')
        lx, ly = xy[-1]
        if role == "rover":
            svg.append(f'<path d="M{lx-4:.1f} {ly:.1f}H{lx+4:.1f} M{lx:.1f} {ly-4:.1f}V{ly+4:.1f}" '
                       f'stroke="{color}" stroke-width="2" stroke-linecap="round"/>')
        else:
            svg.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="3.5" fill="{color}" '
                       f'stroke="#0f1419" stroke-width="1"/>')
        labels.append((lx, ly, sat, color))
    for lx, ly, sat, color in labels:
        svg.append(f'<text x="{lx:.1f}" y="{ly-6:.1f}" fill="#c5d1e0" font-size="11" '
                   f'text-anchor="middle">{sat}</text>')
    nsat = len(bysat)
    npts = sum(len(v["pts"]) for v in bysat.values())
    sub = (f'{nsat} 颗卫星 · {npts} 个轨迹点 · 外圈10°/内圈75° 橙色虚线为常用高度角'
           + (' · ✚ 末点=测站' if role == 'rover' else ' · ● 末点=基站'))
    return f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<title>{title} · 轨迹叠加天空图</title>
<style>body{{margin:0;background:#0b0f14;color:#e6edf3;
font-family:-apple-system,"PingFang SC","Microsoft YaHei",Segoe UI,sans-serif;
display:flex;flex-direction:column;align-items:center;padding:18px}}
h1{{font-size:16px;margin:0 0 2px}} .sub{{color:#8b9bb0;font-size:12px;margin-bottom:12px}}
svg{{max-width:96vw;height:auto;background:#0f1419;border:1px solid #2c3a48;border-radius:10px}}</style>
</head><body><h1>{title} · 卫星轨迹叠加</h1><div class="sub">{sub}</div>
<svg viewBox="0 0 640 640" xmlns="http://www.w3.org/2000/svg">{''.join(svg)}</svg>
</body></html>"""


# --------------------------------------------------------------------------- top-level export
def do_export(base_dir, exports_dir, sid, start_ts, end_ts):
    """执行导出: 写服务端 exports/<sid>/<range>/ 并打包 zip。

    返回 {ok, label, server_dir, zip_path, zip_name, channels:[...], total_periods}。
    """
    label = _range_label(start_ts, end_ts)
    out_root = os.path.join(exports_dir, rec._safe(sid), label)
    os.makedirs(out_root, exist_ok=True)

    channels, zip_members = [], []     # zip_members: (arcname, bytes)
    total = 0
    for d, cdir, items in _channel_dirs(base_dir, sid):
        periods = _filter_periods(items, start_ts, end_ts)
        if not periods:
            continue
        role = items[0].get("role")
        mount = items[0].get("mount")
        folder = rec._safe(mount or d)          # 按挂载点名分文件夹
        sub = os.path.join(out_root, folder)
        os.makedirs(sub, exist_ok=True)

        rtcm = _merge_rtcm(cdir, periods)
        csv = _merge_track_csv(cdir, periods)
        pts = read_track_points(base_dir, sid, start_ts, end_ts).get(d, {}).get("points", [])
        html = build_skyplot_html(f"{role or ''} {mount or d}".strip(), pts, role)

        files = {f"{label}.rtcm": rtcm,
                 f"{label}_track.csv": csv,
                 f"{label}_skyplot.html": html.encode("utf-8")}
        for fn, data in files.items():
            with open(os.path.join(sub, fn), "wb") as fh:
                fh.write(data)
            zip_members.append((f"{folder}/{fn}", data))

        total += len(periods)
        channels.append({"dir": d, "role": role, "mount": mount,
                         "folder": folder, "periods": len(periods),
                         "rtcm_bytes": len(rtcm), "track_points": len(pts)})

    # manifest
    manifest = {"sid": sid, "label": label,
                "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts)),
                "end_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_ts)),
                "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "channels": channels}
    mbytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    with open(os.path.join(out_root, "manifest.json"), "wb") as fh:
        fh.write(mbytes)
    zip_members.append(("manifest.json", mbytes))

    # zip (写到 <sid> 目录下, 文件名=range.zip)
    zip_name = f"{label}.zip"
    zip_path = os.path.join(exports_dir, rec._safe(sid), zip_name)
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for arc, data in zip_members:
            zf.writestr(arc, data)
    with open(zip_path, "wb") as fh:
        fh.write(bio.getvalue())

    return {"ok": True, "label": label, "server_dir": out_root,
            "zip_path": zip_path, "zip_name": zip_name,
            "channels": channels, "total_periods": total,
            "empty": not channels}
