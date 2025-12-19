@echo off
REM 金融数据系统生产环境启动脚本

echo 正在启动金融数据系统生产环境...

REM 设置环境变量
set FLASK_HOST=0.0.0.0
set FLASK_PORT=5000
set WAITRESS_THREADS=4

REM 启动系统
python start_production.py

echo 金融数据系统已关闭
pause