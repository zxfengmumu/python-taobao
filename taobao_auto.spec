# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['panel.py'],
    pathex=[],
    binaries=[],
    datas=[('campaign_template.json', '.')],
    hiddenimports=['core', 'core.config', 'core.http_client', 'core.db', 'core.notify', 'browser', 'browser.driver', 'browser.login', 'browser.slider', 'sync', 'sync.base', 'sync.campaigns', 'sync.products', 'sync.orders', 'sync.risk_products', 'sync.tasks'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='taobao_auto',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['taobao.ico'],
)
