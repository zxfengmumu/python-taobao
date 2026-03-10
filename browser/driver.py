"""浏览器初始化 + Cookie 工具 + 浏览器 fetch（GET/POST）。"""
import base64
import time
import logging
from urllib.parse import urlencode

from DrissionPage import Chromium, ChromiumOptions

log = logging.getLogger("taobao_auto")

_FETCH_TIMEOUT_MS = 25_000
_fetch_seq = 0


def setup_browser():
    """初始化浏览器，设置缓存路径和调试端口。返回 tab 并写入 main._tab。"""
    import main

    port = int(main._config["browser_port"])
    co = (
        ChromiumOptions()
        .set_local_port(port)
        .set_user_data_path(main._config["browser_cache_path"])
        .set_argument("--disable-background-timer-throttling")
        .set_argument("--disable-backgrounding-occluded-windows")
        .set_argument("--disable-renderer-backgrounding")
    )
    browser = Chromium(co)
    main._tab = browser.latest_tab
    log.info("浏览器已启动，端口 %s", port)
    return main._tab


def _cookie_nv(c):
    """从 cookie 对象中提取 name 和 value。"""
    if isinstance(c, dict):
        return c["name"], c["value"]
    return c.name, c.value


def get_cookie_str(tab):
    """将浏览器所有 cookie 格式化为字符串。"""
    return "; ".join(
        f"{n}={v}" for n, v in (_cookie_nv(c) for c in tab.cookies())
    )


def get_tb_token(tab):
    """从 cookie 中提取 _tb_token_ 的值。"""
    for c in tab.cookies():
        name, value = _cookie_nv(c)
        if name == "_tb_token_":
            return value
    return None


def ensure_tab_on_alimama():
    """确保浏览器 tab 在 alimama.com 域名下。"""
    import main

    tab = main._tab
    if "alimama.com" not in (tab.url or ""):
        tab.get("https://fuwu.alimama.com/")
        time.sleep(3)


def browser_get_json(url, referer):
    """通过浏览器 fetch 发送 GET 请求，带 AbortController 超时和唯一标识。"""
    import main
    global _fetch_seq

    tab = main._tab
    try:
        tab.set.activate()
    except Exception:
        pass

    ensure_tab_on_alimama()

    _fetch_seq += 1
    key = f"__fs_{_fetch_seq}"

    tab.run_js(
        'window["' + key + '"]={done:false,result:""};'
        'var ac=new AbortController();'
        'var tid=setTimeout(function(){ac.abort();}, ' + str(_FETCH_TIMEOUT_MS) + ');'
        'fetch("' + url + '",{'
        'method:"GET",'
        'headers:{"accept":"*/*","accept-language":"zh-CN,zh;q=0.9",'
        '"bx-v":"2.5.11",'
        '"content-type":"application/x-www-form-urlencoded; charset=UTF-8",'
        '"x-requested-with":"XMLHttpRequest"},'
        'referrer:"' + referer + '",'
        'credentials:"include",'
        'signal:ac.signal'
        '}).then(function(r){return r.text();})'
        '.then(function(t){clearTimeout(tid);window["' + key + '"]={done:true,result:t};})'
        '.catch(function(e){'
        'clearTimeout(tid);window["' + key + '"]={done:true,result:JSON.stringify({error:e.message})};'
        '});'
    )

    for i in range(60):
        if tab.run_js('return window["' + key + '"].done'):
            result = tab.run_js(
                'var r=window["' + key + '"].result;'
                'delete window["' + key + '"];'
                'return r;'
            )
            return result
        if (i + 1) % 20 == 0:
            from browser.login import check_and_solve_slider
            check_and_solve_slider()
        time.sleep(0.5)

    log.error("浏览器 fetch GET 超时（30s）, url=%s", url[:120])
    tab.run_js('delete window["' + key + '"];')
    return None


def browser_post_form(tab, url, form_data, referer):
    """通过浏览器 fetch 发送表单 POST，确保请求特征与真实浏览器一致。"""
    try:
        tab.set.activate()
    except Exception:
        pass

    body_encoded = urlencode(form_data)
    body_b64 = base64.b64encode(body_encoded.encode("ascii")).decode("ascii")

    tab.run_js(
        'window.__fs={done:false,result:""};'
        'window.__fb=atob("' + body_b64 + '");'
    )

    tab.run_js(
        'fetch("' + url + '",{'
        'method:"POST",'
        'headers:{"accept":"*/*","accept-language":"zh-CN,zh;q=0.9",'
        '"bx-v":"2.5.11","cache-control":"no-cache",'
        '"content-type":"application/x-www-form-urlencoded; charset=UTF-8",'
        '"pragma":"no-cache","x-requested-with":"XMLHttpRequest"},'
        'referrer:"' + referer + '",'
        'body:window.__fb,credentials:"include"'
        '}).then(function(r){return r.text();})'
        '.then(function(t){window.__fs={done:true,result:t};})'
        '.catch(function(e){'
        'window.__fs={done:true,result:JSON.stringify({error:e.message})};'
        '});'
    )

    for i in range(60):
        if tab.run_js("return window.__fs.done"):
            result = tab.run_js(
                "var r=window.__fs.result;"
                "delete window.__fs;delete window.__fb;"
                "return r;"
            )
            return result
        if (i + 1) % 20 == 0:
            from browser.login import check_and_solve_slider
            check_and_solve_slider()
        time.sleep(0.5)

    log.error("浏览器 fetch 超时（30s）")
    tab.run_js("delete window.__fs;delete window.__fb;")
    return None
