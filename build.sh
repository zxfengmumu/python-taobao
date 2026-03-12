#!/bin/bash
cd "$(dirname "$0")"

echo "========================================"
echo " 开始打包 taobao_auto"
echo "========================================"

echo "正在关闭已运行的 taobao_auto..."
pkill -f taobao_auto 2>/dev/null || true

ICON_OPT=""
if [ -f "taobao.ico" ]; then
    ICON_OPT="--icon taobao.ico"
fi

pyinstaller --noconfirm \
    --onefile \
    --noconsole \
    --name taobao_auto \
    $ICON_OPT \
    --add-data "campaign_template.json:." \
    --hidden-import core --hidden-import core.config \
    --hidden-import core.http_client --hidden-import core.db \
    --hidden-import core.notify \
    --hidden-import browser --hidden-import browser.driver \
    --hidden-import browser.login --hidden-import browser.slider \
    --hidden-import sync --hidden-import sync.base \
    --hidden-import sync.campaigns --hidden-import sync.products \
    --hidden-import sync.orders --hidden-import sync.risk_products \
    --hidden-import sync.tasks \
    panel.py

if [ $? -ne 0 ]; then
    echo ""
    echo "[失败] 打包出错，请检查以上错误信息"
    exit 1
fi

echo ""
echo "[成功] 打包完成，可执行文件位于 dist/taobao_auto"
echo ""
echo "正在复制 config.json 到 dist 目录..."
cp -f config.json dist/config.json 2>/dev/null || true
echo "[完成] dist 目录已就绪，可直接发布"
echo ""
