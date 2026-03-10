import logging
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

import main
from core.config import load_config, save_config

TAB_FIELDS = {
    "浏览器": [
        ("browser_cache_path", "Chrome 缓存路径"),
        ("browser_port", "调试端口"),
        ("target_url", "目标网址"),
        ("wechat_webhook", "企业微信 Webhook"),
    ],
    "阿里妈妈接口": [
        ("campaign_api_url", "活动创建接口"),
        ("template_config_api_url", "模板配置接口"),
        ("campaign_list_api_url", "活动列表接口"),
        ("product_list_api_url", "商品列表接口"),
        ("order_list_api_url", "订单列表接口"),
    ],
    "ASYX 平台": [
        ("asyx_login_url", "登录接口"),
        ("asyx_username", "用户名"),
        ("asyx_password", "密码"),
        ("cookie_api_url", "Cookie 同步接口"),
        ("campaign_save_api_url", "活动批量保存接口"),
        ("product_save_api_url", "商品批量保存接口"),
        ("order_save_api_url", "订单批量保存接口"),
        ("task_fetch_url", "任务拉取接口"),
        ("task_submit_url", "任务提交接口"),
    ],
    "页面选择器": [
        ("selector_qr_toggle", "二维码切换按钮"),
        ("selector_qr_refresh", "二维码刷新按钮"),
        ("selector_ready_btn", "Ready 按钮"),
        ("selector_slider", "滑块元素"),
        ("selector_slider_container", "滑块容器"),
        ("selector_slider_fail", "滑块失败刷新按钮(逗号分隔)"),
    ],
    "定时任务": [
        ("schedule_time", "每日执行时间 (HH:MM)"),
        ("campaign_name_prefix", "活动名称前缀"),
        ("campaign_duration_days", "活动持续天数"),
        ("order_query_start_time", "订单查询起始时间 (YYYY-MM-DD HH:MM:SS)"),
        ("order_full_sync_range_days", "订单全量对账范围(天)"),
        ("order_status_sync_time", "订单每日对账时间 (HH:MM)"),
        ("product_full_sync_time", "商品每日对账时间 (HH:MM)"),
        ("risk_product_sync_interval_seconds", "风险商品同步间隔(秒)"),
    ],
}


class TextHandler(logging.Handler):
    """将日志写入 tkinter ScrolledText 的自定义 Handler。"""

    def __init__(self, widget):
        super().__init__()
        self.widget = widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        self.widget.after(0, self._append, msg)

    def _append(self, msg):
        self.widget.configure(state=tk.NORMAL)
        self.widget.insert(tk.END, msg)
        self.widget.see(tk.END)
        self.widget.configure(state=tk.DISABLED)


class PanelApp:

    def __init__(self, root):
        self.root = root
        self.root.title("淘宝自动化控制面板")
        self.root.geometry("780x640")
        self.root.minsize(700, 500)

        self._entries = {}
        self._running = False
        self._thread = None

        self._build_config_tabs()
        self._build_control_bar()
        self._build_log_area()
        self._setup_log_handler()
        self._load_and_fill()

    def _build_config_tabs(self):
        """构建 Notebook 配置选项卡。"""
        frame = ttk.LabelFrame(self.root, text="配置管理", padding=8)
        frame.pack(fill=tk.X, padx=10, pady=(10, 0))

        nb = ttk.Notebook(frame)
        nb.pack(fill=tk.X)

        for tab_name, fields in TAB_FIELDS.items():
            tab = ttk.Frame(nb, padding=10)
            nb.add(tab, text=tab_name)
            for row_idx, (key, label) in enumerate(fields):
                ttk.Label(tab, text=label).grid(
                    row=row_idx, column=0, sticky=tk.W, pady=3,
                )
                var = tk.StringVar()
                if "password" in key.lower():
                    entry = ttk.Entry(tab, textvariable=var, width=72, show="*")
                else:
                    entry = ttk.Entry(tab, textvariable=var, width=72)
                entry.grid(row=row_idx, column=1, sticky=tk.EW, padx=(8, 0), pady=3)
                self._entries[key] = var
            tab.columnconfigure(1, weight=1)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(btn_frame, text="保存配置", command=self._on_save).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="重置为默认值", command=self._on_reset).pack(side=tk.RIGHT, padx=(0, 6))

    def _build_control_bar(self):
        """构建脚本控制按钮区。"""
        frame = ttk.LabelFrame(self.root, text="脚本控制", padding=8)
        frame.pack(fill=tk.X, padx=10, pady=8)

        self.btn_start = ttk.Button(frame, text="启动脚本", command=self._on_start)
        self.btn_start.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_stop = ttk.Button(
            frame, text="停止脚本", command=self._on_stop, state=tk.DISABLED,
        )
        self.btn_stop.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_campaign = ttk.Button(
            frame, text="手动创建活动", command=self._on_manual_campaign, state=tk.DISABLED,
        )
        self.btn_campaign.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_sync = ttk.Button(
            frame, text="手动同步数据", command=self._on_manual_sync, state=tk.DISABLED,
        )
        self.btn_sync.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_sync_campaigns = ttk.Button(
            frame, text="同步活动", command=self._on_manual_sync_campaigns, state=tk.DISABLED,
        )
        self.btn_sync_campaigns.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_sync_products = ttk.Button(
            frame, text="同步商品", command=self._on_manual_sync_products, state=tk.DISABLED,
        )
        self.btn_sync_products.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_sync_orders = ttk.Button(
            frame, text="同步订单", command=self._on_manual_sync_orders, state=tk.DISABLED,
        )
        self.btn_sync_orders.pack(side=tk.LEFT, padx=(0, 6))

        self.lbl_status = ttk.Label(frame, text="状态：未运行", foreground="gray")
        self.lbl_status.pack(side=tk.RIGHT)

    def _build_log_area(self):
        """构建日志显示区域。"""
        frame = ttk.LabelFrame(self.root, text="运行日志", padding=8)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.log_text = scrolledtext.ScrolledText(frame, state=tk.DISABLED, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def _setup_log_handler(self):
        """将 taobao_auto 日志绑定到 GUI 文本框。"""
        handler = TextHandler(self.log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger = logging.getLogger("taobao_auto")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def _load_and_fill(self):
        """加载配置并填充到表单。"""
        cfg = load_config()
        for key, var in self._entries.items():
            var.set(str(cfg.get(key, "")))

    def _collect_config(self):
        """从表单收集配置。"""
        cfg = {}
        for key, var in self._entries.items():
            val = var.get().strip()
            _INT_KEYS = (
                "browser_port", "campaign_duration_days",
                "order_full_sync_range_days",
                "risk_product_sync_interval_seconds",
            )
            if key in _INT_KEYS:
                try:
                    val = int(val)
                except ValueError:
                    pass
            cfg[key] = val
        return cfg

    def _on_save(self):
        cfg = self._collect_config()
        save_config(cfg)
        messagebox.showinfo("提示", "配置已保存")

    def _on_reset(self):
        from core.config import DEFAULT_CONFIG
        for key, var in self._entries.items():
            var.set(str(DEFAULT_CONFIG.get(key, "")))

    def _on_start(self):
        if self._running:
            return
        cfg = self._collect_config()
        save_config(cfg)
        main.init(cfg)

        self._running = True
        self._update_buttons()

        self._thread = threading.Thread(target=self._run_script, daemon=True)
        self._thread.start()

    def _run_script(self):
        """子线程执行入口。"""
        try:
            main.run()
        except Exception as e:
            logging.getLogger("taobao_auto").error("脚本异常退出: %s", e)
        finally:
            self._running = False
            self.root.after(0, self._update_buttons)

    def _on_stop(self):
        if not self._running:
            return
        main.stop()
        self.lbl_status.config(text="状态：正在停止...", foreground="orange")

    def _on_manual_campaign(self):
        if not self._running:
            return
        from sync.campaigns import create_campaign
        threading.Thread(
            target=lambda: main.run_guarded_task("create_campaign", create_campaign),
            daemon=True,
        ).start()

    def _on_manual_sync(self):
        if not self._running:
            return
        threading.Thread(target=self._sync_products_then_orders, daemon=True).start()

    def _sync_products_then_orders(self):
        from sync.products import fetch_new_products, push_pending_products
        from sync.orders import fetch_new_orders, push_pending_orders
        main.run_guarded_task("fetch_products", fetch_new_products)
        main.run_guarded_task("push_products", push_pending_products)
        main.run_guarded_task("fetch_orders", fetch_new_orders)
        main.run_guarded_task("push_orders", push_pending_orders)

    def _on_manual_sync_campaigns(self):
        if not self._running:
            return
        from sync.campaigns import sync_campaigns
        threading.Thread(
            target=lambda: main.run_guarded_task("sync_campaigns", sync_campaigns),
            daemon=True,
        ).start()

    def _on_manual_sync_orders(self):
        if not self._running:
            return
        threading.Thread(target=self._sync_orders_full, daemon=True).start()

    def _sync_orders_full(self):
        from sync.orders import fetch_new_orders, push_pending_orders
        main.run_guarded_task("fetch_orders", fetch_new_orders)
        main.run_guarded_task("push_orders", push_pending_orders)

    def _on_manual_sync_products(self):
        if not self._running:
            return
        threading.Thread(target=self._sync_products_full, daemon=True).start()

    def _sync_products_full(self):
        from sync.products import fetch_new_products, push_pending_products
        main.run_guarded_task("fetch_products", fetch_new_products)
        main.run_guarded_task("push_products", push_pending_products)

    def _update_buttons(self):
        running_state = tk.NORMAL if self._running else tk.DISABLED
        stopped_state = tk.DISABLED if self._running else tk.NORMAL

        self.btn_start.config(state=stopped_state)
        self.btn_stop.config(state=running_state)
        self.btn_campaign.config(state=running_state)
        self.btn_sync.config(state=running_state)
        self.btn_sync_campaigns.config(state=running_state)
        self.btn_sync_orders.config(state=running_state)
        self.btn_sync_products.config(state=running_state)

        if self._running:
            self.lbl_status.config(text="状态：运行中", foreground="green")
        else:
            self.lbl_status.config(text="状态：未运行", foreground="gray")


def main_gui():
    root = tk.Tk()
    PanelApp(root)
    root.mainloop()


if __name__ == "__main__":
    main_gui()
