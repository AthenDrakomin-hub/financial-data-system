@echo off
REM 本地开发测试环境启动脚本

echo 正在启动金融数据系统开发测试环境...

REM 启动Elasticsearch Docker容器（如果尚未运行）
echo 检查Elasticsearch容器状态...
docker ps | findstr elasticsearch >nul
if %errorlevel% equ 0 (
    echo Elasticsearch容器已在运行
) else (
    echo 启动Elasticsearch容器...
    docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:8.14.0
    timeout /t 10 /nobreak >nul
)

REM 启动金融数据系统开发服务器
echo 启动金融数据系统...
python finance_data_system_elastic.py

echo 金融数据系统已关闭
pause