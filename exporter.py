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


# --------------------------------------------------------------------------- import (反向: 解析离线轨迹文件)
def _role_from_name(s):
    s = (s or "").lower()
    return "rover" if ("rover" in s or "测" in s) else "base"


def parse_track_csv(text):
    """解析导出的 `_track.csv` (time_utc,sat,sys,prn,az_deg,el_deg,cn0) -> 轨迹点。"""
    pts = []
    for ln in text.splitlines()[1:]:          # 跳表头
        f = ln.split(",")
        if len(f) < 7:
            continue
        t, sat, sysc, _prn, az, el, cn0 = f[:7]
        if not az or not el:
            continue
        try:
            pts.append([_parse_iso(t) or 0, sat, sysc, float(az), float(el),
                        float(cn0) if cn0 else None])
        except Exception:
            pass
    pts.sort(key=lambda r: (r[1], r[0]))
    return pts


def import_tracks(filename, data):
    """从上传的 `_track.csv` 或导出 zip 解析轨迹, 返回与 read_track_points 同形的
    {key: {role, mount, npoints, points}}。zip 内按 manifest.json 还原 role/挂载点。"""
    name = (filename or "").lower()
    res = {}
    if name.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            rolemap = {}
            try:
                mani = json.loads(zf.read("manifest.json").decode("utf-8"))
                for c in mani.get("channels", []):
                    rolemap[c.get("folder")] = (c.get("role"), c.get("mount"))
            except Exception:
                pass
            for n in zf.namelist():
                if not n.endswith("_track.csv"):
                    continue
                folder = n.split("/")[0] if "/" in n else n
                role, mount = rolemap.get(folder, (None, folder))
                pts = parse_track_csv(zf.read(n).decode("utf-8", "replace"))
                res[folder] = {"role": role or _role_from_name(folder),
                               "mount": mount or folder,
                               "npoints": len(pts), "points": pts}
    else:
        key = os.path.basename(filename or "imported")
        body = data.decode("utf-8", "replace") if isinstance(data, bytes) else data
        pts = parse_track_csv(body)
        res[key] = {"role": _role_from_name(name), "mount": key,
                    "npoints": len(pts), "points": pts}
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
    """用区间内的卫星位置点生成自包含「按周期回放」天空图 (SVG + HTML)。

    点数据内嵌为 JSON; 时间轴每格=一个 10min 周期, 只画该周期的卫星位置,
    拖动/播放可看卫星行进与 CN0 变化。颜色按星座、大小按 CN0, base=空心圆○、rover=十字✚。"""
    cx = cy = 320
    R = 300
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
    bg = ''.join(svg)
    # 内嵌点数据 (仅保留有效高度角)，由 JS 按周期渲染
    data = [[int(t), sat, sysc, round(az, 2), round(el, 2),
             (round(cn0, 1) if cn0 is not None else None)]
            for (t, sat, sysc, az, el, cn0) in points
            if el is not None and el >= 0]
    data_json = json.dumps(data, separators=(",", ":"))
    color_json = json.dumps(_SYS_COLOR, separators=(",", ":"))
    role_js = "rover" if role == "rover" else "base"
    nsat = len({d[1] for d in data})
    nper = len({d[0] // 600 for d in data})
    sub = (f'{nsat} 颗卫星 · {nper} 个周期 · 点大小=载噪比CN0 · 外圈10°/内圈75° 橙色虚线为常用高度角'
           + (' · ✚ 测站' if role == 'rover' else ' · ○ 基站'))
    return f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<title>{title} · 轨迹叠加天空图</title>
<style>body{{margin:0;background:#0b0f14;color:#e6edf3;
font-family:-apple-system,"PingFang SC","Microsoft YaHei",Segoe UI,sans-serif;
display:flex;flex-direction:column;align-items:center;padding:18px}}
h1{{font-size:16px;margin:0 0 2px}} .sub{{color:#8b9bb0;font-size:12px;margin-bottom:10px}}
.wrap{{position:relative;width:min(96vw,720px)}}
#rst{{position:absolute;top:8px;right:8px;z-index:2;background:#222c38;color:#8b9bb0;
border:1px solid #2c3a48;border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer}}
.hint{{position:absolute;top:11px;left:12px;z-index:2;color:#8b9bb0;font-size:11px;pointer-events:none}}
svg{{width:100%;height:auto;background:#0f1419;border:1px solid #2c3a48;border-radius:10px;
cursor:grab;touch-action:none;user-select:none;display:block}}
svg.grabbing{{cursor:grabbing}}
.tl{{display:flex;align-items:center;gap:10px;width:min(96vw,720px);margin-top:10px}}
.tl input[type=range]{{flex:1}}
.tl button{{background:#222c38;color:#8b9bb0;border:1px solid #2c3a48;border-radius:6px;
padding:4px 12px;font-size:13px;cursor:pointer}}
.tl span{{color:#8b9bb0;font-size:12px;white-space:nowrap;min-width:170px;text-align:right;
font-variant-numeric:tabular-nums}}</style>
</head><body><h1>{title} · 卫星轨迹回放</h1><div class="sub">{sub}</div>
<div class="wrap"><button id="rst">复位</button><span class="hint">滚轮缩放 · 拖拽平移</span>
<svg id="sky" viewBox="0 0 640 640" xmlns="http://www.w3.org/2000/svg"><g id="c">{bg}<g id="marks"></g></g></svg></div>
<div class="tl"><button id="play">▶</button>
<input id="tl" type="range" min="0" max="0" value="0" step="1">
<span id="tlab"></span></div>
<script>(function(){{
var svg=document.getElementById('sky'),g=document.getElementById('c'),marks=document.getElementById('marks'),
sc=1,tx=0,ty=0,cx=320,cy=320,R=300,ROLE="{role_js}",PTS={data_json},SYS={color_json};
function ap(){{g.setAttribute('transform','translate('+tx+' '+ty+') scale('+sc+')');}}
function pt(e){{var p=svg.createSVGPoint();p.x=e.clientX;p.y=e.clientY;
var m=svg.getScreenCTM();return m?p.matrixTransform(m.inverse()):{{x:0,y:0}};}}
function proj(az,el){{var r=R*(90-el)/90,a=az*Math.PI/180;return [cx+r*Math.sin(a),cy-r*Math.cos(a)];}}
function rad(c){{if(c==null)return 3;var v=Math.max(20,Math.min(50,c));return 3+(v-20)/30*9;}}
function esc(s){{return String(s).replace(/[&<>]/g,function(m){{return {{'&':'&amp;','<':'&lt;','>':'&gt;'}}[m];}});}}
var pset={{}};PTS.forEach(function(p){{pset[Math.floor(p[0]/600)]=1;}});
var periods=Object.keys(pset).map(Number).sort(function(a,b){{return a-b;}});
function render(idx){{
  if(!periods.length){{marks.innerHTML='';return;}}
  idx=Math.max(0,Math.min(periods.length-1,idx|0));
  var pk=periods[idx],seen={{}},out='';
  PTS.forEach(function(p){{var t=p[0],sat=p[1],sys=p[2],az=p[3],el=p[4],cn0=p[5];
    if(Math.floor(t/600)!==pk)return;
    var color=SYS[sys]||'#8b9bb0',xy=proj(az,el),x=xy[0],y=xy[1],r=rad(cn0),
      tip=esc(sat)+(cn0!=null?' · '+Math.round(cn0)+' dBHz':'');
    if(ROLE==='rover'){{var L=r+2;
      out+='<path d="M'+(x-L).toFixed(1)+' '+y.toFixed(1)+'H'+(x+L).toFixed(1)+' M'+x.toFixed(1)+' '+(y-L).toFixed(1)+'V'+(y+L).toFixed(1)+'" stroke="'+color+'" stroke-width="2" stroke-linecap="round"><title>'+tip+'</title></path>';
    }}else out+='<circle cx="'+x.toFixed(1)+'" cy="'+y.toFixed(1)+'" r="'+r.toFixed(1)+'" fill="none" stroke="'+color+'" stroke-width="1.6"><title>'+tip+'</title></circle>';
    if(!seen[sat]){{seen[sat]=1;out+='<text x="'+x.toFixed(1)+'" y="'+(y-r-3).toFixed(1)+'" fill="#c5d1e0" font-size="11" text-anchor="middle">'+esc(sat)+'</text>';}}
  }});
  marks.innerHTML=out;
  var d=new Date(pk*600*1000);
  document.getElementById('tlab').textContent=d.toLocaleString('zh-CN',{{hour12:false}})+' · '+(idx+1)+'/'+periods.length;
  var sl=document.getElementById('tl');if(+sl.value!==idx)sl.value=idx;
}}
var sl=document.getElementById('tl');sl.max=Math.max(0,periods.length-1);sl.value=Math.max(0,periods.length-1);
sl.addEventListener('input',function(){{render(+this.value);}});
var timer=null,pb=document.getElementById('play');
pb.addEventListener('click',function(){{if(timer){{clearInterval(timer);timer=null;pb.textContent='▶';return;}}
  if(!periods.length)return;pb.textContent='⏸';
  timer=setInterval(function(){{var i=(+sl.value)+1;if(i>periods.length-1)i=0;render(i);}},800);}});
render(periods.length?periods.length-1:0);
svg.addEventListener('wheel',function(e){{e.preventDefault();var P=pt(e),
f=e.deltaY<0?1.15:1/1.15,ns=Math.min(10,Math.max(1,sc*f));
tx=P.x-(ns/sc)*(P.x-tx);ty=P.y-(ns/sc)*(P.y-ty);sc=ns;ap();}},{{passive:false}});
var pan=false,ps=null;
svg.addEventListener('mousedown',function(e){{if(e.button!==0)return;pan=true;ps=pt(e);svg.classList.add('grabbing');}});
window.addEventListener('mousemove',function(e){{if(!pan)return;var P=pt(e);tx+=P.x-ps.x;ty+=P.y-ps.y;ps=P;ap();}});
window.addEventListener('mouseup',function(){{pan=false;svg.classList.remove('grabbing');}});
document.getElementById('rst').addEventListener('click',function(){{sc=1;tx=0;ty=0;ap();}});
}})();</script>
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
