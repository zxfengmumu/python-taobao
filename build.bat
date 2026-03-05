@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo  开始打包 taobao_auto
echo ========================================

echo 正在关闭已运行的 taobao_auto.exe...
taskkill /f /im taobao_auto.exe >nul 2>&1

pyinstaller --noconfirm ^
    --onefile ^
    --windowed ^
    --name taobao_auto ^
    --icon "taobao.ico" ^
    --add-data "campaign_template.json;." ^
    panel.py

if %errorlevel% neq 0 (
    echo.
    echo [失败] 打包出错，请检查以上错误信息
    pause
    exit /b 1
)

echo.
echo [成功] 打包完成，exe 位于 dist\taobao_auto.exe
echo.
echo 正在复制 config.json 到 dist 目录...
copy /y config.json dist\config.json >nul
echo [完成] dist 目录已就绪，可直接发布
echo.
pause
