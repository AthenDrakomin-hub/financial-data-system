@echo off
REM 将金融数据系统注册为Windows服务的脚本
REM 注意：需要以管理员身份运行

echo 正在检查NSSM是否已安装...

where nssm >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误：未找到NSSM (Non-Sucking Service Manager)
    echo 请先下载并安装NSSM:
    echo https://nssm.cc/download
    echo.
    echo 安装完成后，请以管理员身份重新运行此脚本
    pause
    exit /b 1
)

echo NSSM已安装，正在注册金融数据系统服务...

REM 设置服务名称
set SERVICE_NAME=FinanceDataService

REM 获取当前目录
set CURRENT_DIR=%~dp0

REM 注册服务
nssm install %SERVICE_NAME% "%CURRENT_DIR%start_production.bat"

REM 设置工作目录
nssm set %SERVICE_NAME% AppDirectory "%CURRENT_DIR%"

REM 设置启动参数（可选）
nssm set %SERVICE_NAME% AppParameters ""

echo 服务注册完成！
echo.
echo 要启动服务，请运行：net start %SERVICE_NAME%
echo 要停止服务，请运行：net stop %SERVICE_NAME%
echo 要删除服务，请运行：nssm remove %SERVICE_NAME%

pause