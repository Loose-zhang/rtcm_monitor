#!/usr/bin/env bash
# RTCM Monitor launcher
cd "$(dirname "$0")"
python3 -m pip install -q -r requirements.txt
echo "打开浏览器访问 http://127.0.0.1:7999"
python3 app.py
