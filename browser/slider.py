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
    ".nc_wrapper > .errloading",
]


def _has_slider_fail_indicator(ctx, config):
    """仅检测是否存在滑块失败标志元素（不点击）。"""
    selectors = list(_SLIDER_FAIL_SELECTORS)
    custom = config.get("selector_slider_fail", "")
    if custom:
        selectors.insert(0, custom)
    return any(js_exists(ctx, s) for s in selectors)


def _detect_slider_fail(ctx, tab, config):
    """检测滑块验证是否失败，通过 CDP actions 点击重试按钮（isTrusted=true）。"""
    custom = config.get("selector_slider_fail", "")
    selectors = ([custom] if custom else []) + _SLIDER_FAIL_SELECTORS
    seen = set()

    for selector in selectors:
        if selector in seen:
            continue
        seen.add(selector)
        try:
            if not js_exists(ctx, selector):
                continue
            time.sleep(0.3)

            drission_sel = f"css:{selector}" if " " in selector else selector
            ele = ctx.ele(drission_sel, timeout=1)
            if not ele:
                continue
            tab.actions.move_to(ele)
            time.sleep(random.uniform(0.1, 0.3))
            tab.actions.click()
            log.info("通过 actions 点击了滑块重试按钮: %s", selector)
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
        if _detect_slider_fail(ctx, tab, config):
            log.info("容器不可见但检测到验证失败按钮，已点击重试")
            return True
        return False

    if _detect_slider_fail(ctx, tab, config):
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
            log.info("检测到失败标志，尝试点击重试...")
            _detect_slider_fail(ctx, tab, config)
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
            if not _has_slider_fail_indicator(ctx, config):
                log.info("滑块验证通过（容器已消失且无失败标志）")
                return True
            log.info("容器消失但检测到失败标志，尝试点击重试...")
            _detect_slider_fail(ctx, tab, config)
            time.sleep(1.5)
            continue

        if _detect_slider_fail(ctx, tab, config):
            log.info("第 %d 次拖动后验证失败，已点击重试", attempt)
            time.sleep(2)
            continue

        log.info("第 %d 次拖动完成，等待验证结果...", attempt)
        time.sleep(1)

    return True


def _human_drag(tab, slider, distance):
    """模拟人类拖动滑块：悬停微调→按下停顿→ease-in-out 曲线拖动→末端停顿→释放。"""
    actions = tab.actions
    actions.move_to(slider)
    time.sleep(random.uniform(0.1, 0.25))

    # 按下前做 1~2 次微小抖动，模拟真人落点调整
    for _ in range(random.randint(1, 2)):
        mx = random.randint(-2, 2)
        my = random.randint(-1, 1)
        actions.move(mx, my, duration=0)
        time.sleep(random.uniform(0.03, 0.07))

    actions.hold()
    time.sleep(random.uniform(0.12, 0.28))

    track = _build_slide_track(distance)
    for dx, dy, dt in track:
        if dx != 0 or dy != 0:
            actions.move(dx, dy, duration=0)
        time.sleep(dt)

    time.sleep(random.uniform(0.05, 0.12))
    actions.release()


def _build_slide_track(distance):
    """构建 ease-in-out 滑动轨迹，返回 [(dx, dy, sleep_sec), ...]。"""
    num_steps = random.randint(50, 80)
    total_time = random.uniform(0.9, 1.5)
    base_dt = total_time / num_steps

    overshoot = random.randint(3, 8)
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
    prev_x = 0
    for i in range(1, len(positions)):
        dx = positions[i] - prev_x
        if dx < 0:
            dx = 0
        prev_x = positions[i]
        is_tail = (i > num_steps * 0.85)
        dy = random.choice([-1, -1, 0, 0, 1, 1]) if is_tail else random.choice([-1, 0, 0, 0, 0, 1])
        speed_factor = random.uniform(1.1, 1.6) if is_tail else random.uniform(0.85, 1.15)
        dt = base_dt * speed_factor
        track.append((dx, dy, max(dt, 0.008)))

    remaining = main_distance - prev_x
    if remaining > 0:
        track.append((remaining, 0, base_dt * random.uniform(1.0, 1.4)))

    if overshoot > 0:
        track.append((-overshoot, random.choice([-1, 0, 1]), random.uniform(0.04, 0.1)))

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
