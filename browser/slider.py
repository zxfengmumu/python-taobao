"""滑块验证码检测与自动处理。"""
import time
import random
import logging

log = logging.getLogger("taobao_auto")


def _js_safe(css_selector):
    """将 CSS 选择器中的双引号替换为单引号，避免嵌入 JS 字符串时引号冲突。"""
    return css_selector.replace('"', "'")


def js_click(ctx, css_selector):
    """用 JS querySelector 查找并点击元素，成功返回 True。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'var el = document.querySelector("{safe}");'
        'if(el){el.click(); return true;} return false;'
    ))


def js_exists(ctx, css_selector):
    """用 JS querySelector 检测元素是否存在。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'return !!document.querySelector("{safe}")'
    ))


def js_visible(ctx, css_selector):
    """用 JS 检测元素是否存在且可见（有尺寸、未隐藏）。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'var el = document.querySelector("{safe}");'
        'if(!el) return false;'
        'var r = el.getBoundingClientRect();'
        'if(r.width === 0 || r.height === 0) return false;'
        'var s = getComputedStyle(el);'
        'return s.display !== "none" && s.visibility !== "hidden";'
    ))


_SLIDER_FAIL_SELECTORS = [
    "#nc_1_refresh1",
    ".nc-lang-cnt a",
    ".errloading a",
    ".btn-refresh",
    ".nc_iconfont.btn_refresh",
]


def _has_slider_fail_indicator(ctx, config):
    """仅检测是否存在滑块失败标志元素（不点击）。"""
    selectors = list(_SLIDER_FAIL_SELECTORS)
    custom = config.get("selector_slider_fail", "")
    if custom:
        selectors.insert(0, custom)
    return any(js_exists(ctx, s) for s in selectors)


def _detect_slider_fail(ctx, config):
    """检测滑块验证是否失败，依次尝试多个选择器查找并点击重试/刷新按钮。"""
    custom = config.get("selector_slider_fail", "")
    selectors = ([custom] if custom else []) + _SLIDER_FAIL_SELECTORS
    seen = set()

    for selector in selectors:
        if selector in seen:
            continue
        seen.add(selector)
        try:
            if not js_visible(ctx, selector):
                continue
            time.sleep(0.3)

            safe = _js_safe(selector)
            dispatched = ctx.run_js(
                f'var el = document.querySelector("{safe}");'
                'if(el){'
                '  el.click();'
                '  el.dispatchEvent(new MouseEvent("click",{bubbles:true,cancelable:true}));'
                '  return true;'
                '} return false;'
            )
            if dispatched:
                log.info("通过 JS dispatchEvent 点击了滑块重试按钮: %s", selector)
                return True

            ele = ctx.ele(selector, timeout=1)
            if ele:
                ele.click()
                log.info("通过 DrissionPage 点击了滑块重试按钮: %s", selector)
                return True
        except Exception:
            continue
    return False


def _iter_all_frames(tab):
    """遍历页面中所有 iframe，返回 frame 对象的生成器。"""
    try:
        iframe_eles = tab.eles("tag:iframe")
        if not iframe_eles:
            return
        for ele in iframe_eles:
            try:
                frame = tab.get_frame(ele)
                if frame:
                    yield frame
            except Exception:
                continue
    except Exception:
        try:
            frame = tab.get_frame("tag:iframe")
            if frame:
                yield frame
        except Exception:
            pass


def _try_solve_slider_in(ctx, tab, config):
    """在指定上下文（主页面或 iframe）中检测并处理滑块，最多重试 3 次。"""
    sel_slider = config.get("selector_slider", "#nc_1_n1z")
    sel_container = config.get("selector_slider_container", "#nc_1__scale_text")
    max_attempts = 3

    if not js_visible(ctx, sel_container):
        if _detect_slider_fail(ctx, config):
            log.info("容器不可见但检测到验证失败按钮，已点击重试")
            return True
        return False

    if _detect_slider_fail(ctx, config):
        log.info("检测到滑块验证失败状态，点击刷新重试...")
        time.sleep(1.5)

    for attempt in range(1, max_attempts + 1):
        if not js_visible(ctx, sel_slider):
            if not js_exists(ctx, sel_container):
                log.info("滑块容器已消失，验证可能已通过")
                return True
            if not _has_slider_fail_indicator(ctx, config):
                log.info("滑块不可见且无失败标志，视为正常状态")
                return False
            log.info("滑块验证失败后等待重新加载...")
            time.sleep(1.5)
            continue

        slider = ctx.ele(sel_slider, timeout=1)
        if not slider:
            continue
        container = ctx.ele(sel_container, timeout=1)
        if not container:
            continue

        log.info("滑块验证码第 %d 次拖动尝试...", attempt)
        container_w = container.rect.size[0]
        slider_w = slider.rect.size[0]
        distance = int(container_w - slider_w) + random.randint(3, 10)
        log.info("容器宽度=%d, 滑块宽度=%d, 拖动距离=%d", container_w, slider_w, distance)

        _human_drag(tab, slider, distance)
        time.sleep(2)

        if not js_exists(ctx, sel_container):
            log.info("滑块验证通过（容器已消失）")
            return True

        if _detect_slider_fail(ctx, config):
            log.info("第 %d 次拖动后验证失败，已点击重试", attempt)
            time.sleep(2)
            continue

        log.info("第 %d 次拖动完成，等待验证结果...", attempt)
        time.sleep(1)

    return True


def _human_drag(tab, slider, distance):
    """模拟人类拖动滑块：按下停顿→ease-in-out 曲线拖动→末端停顿→释放。"""
    actions = tab.actions
    actions.move_to(slider)
    time.sleep(random.uniform(0.08, 0.2))
    actions.hold()
    time.sleep(random.uniform(0.1, 0.25))

    track = _build_slide_track(distance)
    for dx, dy, dt in track:
        actions.move(dx, dy)
        time.sleep(dt)

    time.sleep(random.uniform(0.05, 0.15))
    actions.release()


def _build_slide_track(distance):
    """构建 ease-in-out 滑动轨迹，返回 [(dx, dy, sleep_sec), ...]。"""
    num_steps = random.randint(8, 15)
    total_time = random.uniform(0.3, 0.6)
    base_dt = total_time / num_steps

    overshoot = random.randint(2, 6)
    main_distance = distance + overshoot

    positions = []
    for i in range(num_steps + 1):
        t = i / num_steps
        if t < 0.5:
            ease = 4 * t * t * t
        else:
            ease = 1 - pow(-2 * t + 2, 3) / 2
        positions.append(round(ease * main_distance))

    track = []
    for i in range(1, len(positions)):
        dx = positions[i] - positions[i - 1]
        if dx <= 0:
            continue
        is_tail = (i > num_steps * 0.85)
        dy = random.choice([-1, -1, 0, 0, 0, 1, 1]) if is_tail else random.choice([-1, 0, 0, 0, 0, 0, 1])
        speed_factor = random.uniform(1.2, 1.8) if is_tail else random.uniform(0.8, 1.2)
        dt = base_dt * speed_factor
        track.append((dx, dy, max(dt, 0.008)))

    remaining = main_distance - positions[-1]
    if remaining > 0:
        track.append((remaining, 0, base_dt * random.uniform(1.0, 1.5)))

    if overshoot > 0:
        track.append((-overshoot, random.choice([-1, 0, 1]), random.uniform(0.03, 0.08)))

    return track


def check_and_solve_slider(tab, config):
    """检测主页面和所有 iframe 中的滑块验证码并自动处理。"""
    try:
        if _try_solve_slider_in(tab, tab, config):
            return True
    except Exception as e:
        log.warning("主页面滑块检测异常: %s", e)

    for frame in _iter_all_frames(tab):
        try:
            if _try_solve_slider_in(frame, tab, config):
                return True
        except Exception as e:
            log.warning("iframe 滑块检测异常: %s", e)

    return False
