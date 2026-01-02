import tkinter as tk
from tkinter import ttk, messagebox
import feedparser
import requests
import re
from threading import Thread
from io import BytesIO
from PIL import Image, ImageTk
import hashlib
import json
import os
from datetime import datetime

# ====== 配置区 ======
QB_LOGIN_URL = "http://192.168.50.3:8080/api/v2/auth/login"
QB_ADD_TORRENT_URL = "http://192.168.50.3:8080/api/v2/torrents/add"
USERNAME = "admin"
PASSWORD = "xxx"  # ← 替换为你的密码！

PRESET_RSS = {
    "mikanime": {
        "url": "https://mikanime.tv/RSS/MyBangumi?token=2iwXTp1m89Rxj92aJIfIrA%3d%3d",
        "include": r"1月|新番|ani",
        "exclude": r"英语|巴哈"
    },
    "moe": {
        "url": "https://192.168.50.4/rss.xml",
        "include": r"中文",
        "exclude": r"英文"
    },
}

# ====== 常量 ======
IMAGE_WIDTH = 150
IMAGE_HEIGHT = 150
TEXT_WRAP_LENGTH = 800
IMAGE_TIMEOUT = 15
PAGE_SIZE = 50
MAX_DISPLAY_ENTRIES = 200
HISTORY_FILE = "rss.log"


# ==================== 工具函数 ====================

def extract_magnet_links(text):
    """从文本中提取磁力链接"""
    if not text:
        return []
    magnets = set()
    magnet_pattern = r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+(?:&[a-zA-Z0-9%._\-]*)*)'
    magnets.update(re.findall(magnet_pattern, text, re.IGNORECASE))
    
    infohash_pattern = r'\b([a-fA-F0-9]{40})\b'
    for h in re.findall(infohash_pattern, text):
        if len(h) == 40:
            magnets.add(f"magnet:?xt=urn:btih:{h.lower()}")
    return list(magnets)


def extract_image_url_from_html(html):
    """从 HTML 字符串中提取第一个 <img src>"""
    if not html:
        return None
    match = re.search(r'<img[^>]+src\s*=\s*["\']([^"\']+)["\'][^>]*>', html, re.IGNORECASE)
    return match.group(1) if match else None


def extract_image_url(item):
    """从 feedparser item 中提取图片 URL"""
    # 优先：media:thumbnail
    if hasattr(item, 'media_thumbnail') and item.media_thumbnail:
        for thumb in item.media_thumbnail:
            url = thumb.get('url')
            if url:
                return url

    # 其次：content
    if hasattr(item, 'content'):
        for c in item.content:
            if c.type in ('text/html', 'xhtml', 'application/xhtml+xml'):
                img = extract_image_url_from_html(c.value)
                if img:
                    return img

    # 再次：summary / description
    for field in ['summary', 'description']:
        value = getattr(item, field, '')
        if value:
            img = extract_image_url_from_html(value)
            if img:
                return img
    return None


def normalize_infohash(magnet):
    """标准化 infohash (Base32 -> Hex)"""
    match = re.search(r'btih:([a-zA-Z0-9]{32,40})', magnet, re.IGNORECASE)
    if not match:
        return None
    ih = match.group(1).lower()
    if len(ih) == 32:
        try:
            import base64
            decoded = base64.b32decode(ih.upper())
            ih = decoded.hex()
        except Exception:
            return None
    return ih if len(ih) == 40 else None


def matches_filter(title, summary, include_pat, exclude_pat):
    """检查标题和摘要是否匹配过滤器"""
    full_text = f"{title or ''} {summary or ''}"
    
    if exclude_pat.strip():
        try:
            if re.search(exclude_pat, full_text, re.IGNORECASE):
                return False
        except re.error:
            pass
    
    if include_pat.strip():
        try:
            return bool(re.search(include_pat, full_text, re.IGNORECASE))
        except re.error:
            return False
    return True


def parse_rss_time(time_str):
    """解析 RSS 时间字符串"""
    if not time_str:
        return "未知时间"
    
    formats = [
        '%a, %d %b %Y %H:%M:%S %z',
        '%a, %d %b %Y %H:%M:%S %Z',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str.strip(), fmt)
            return dt.strftime('%Y-%m-%d %H:%M')
        except ValueError:
            continue
    
    try:
        parsed = feedparser._parse_date(time_str)
        if parsed:
            dt = datetime(*parsed[:6])
            return dt.strftime('%Y-%m-%d %H:%M')
    except Exception:
        pass
    return "未知时间"


# ==================== 历史记录管理 ====================

class HistoryManager:
    """历史记录管理器"""
    
    @staticmethod
    def _read_all_entries():
        """读取所有历史记录"""
        if not os.path.exists(HISTORY_FILE):
            return []
        
        entries = []
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entries.append(json.loads(line.strip()))
                    except Exception:
                        continue
        except Exception:
            pass
        return entries
    
    @staticmethod
    def _write_all_entries(entries):
        """写入所有历史记录"""
        try:
            with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                for e in entries:
                    f.write(json.dumps(e, ensure_ascii=False) + '\n')
        except Exception:
            pass
    
    @staticmethod
    def load_by_rss_name(rss_name):
        """加载指定 RSS 源的历史记录"""
        entries = HistoryManager._read_all_entries()
        
        # 过滤并只保留最新的记录
        history = {e['infohash']: e for e in entries if e.get('rss_name') == rss_name and e.get('infohash')}
        entries_list = sorted(history.values(), key=lambda x: x.get('timestamp', ''), reverse=True)
        return {e['infohash']: e for e in entries_list[:MAX_DISPLAY_ENTRIES]}
    
    @staticmethod
    def save(new_entries, rss_name):
        """保存新条目到历史记录"""
        # 读取现有记录
        entries = HistoryManager._read_all_entries()
        existing = {(e.get('rss_name'), e.get('infohash')): e for e in entries if e.get('rss_name') and e.get('infohash')}
        
        # 合并新条目：用最新数据覆盖，但只保留 selected 状态
        now_iso = datetime.now().isoformat()
        for e in new_entries:
            key = (rss_name, e['infohash'])
            old_selected = existing.get(key, {}).get('selected', False)
            
            # 用新数据覆盖
            out_entry = e.copy()
            out_entry['rss_name'] = rss_name
            out_entry['timestamp'] = now_iso
            out_entry['selected'] = old_selected
            existing[key] = out_entry
        
        # 按时间排序后写入
        all_entries = sorted(existing.values(), key=lambda x: x.get('timestamp', ''))
        HistoryManager._write_all_entries(all_entries)
    
    @staticmethod
    def mark_as_selected(rss_name, infohashes):
        """标记条目为已下载"""
        entries = HistoryManager._read_all_entries()
        
        # 更新选中状态
        infohash_set = set(infohashes)
        for entry in entries:
            if entry.get('rss_name') == rss_name and entry.get('infohash') in infohash_set:
                entry['selected'] = True
        
        HistoryManager._write_all_entries(entries)
    
    @staticmethod
    def clear_for_rss(rss_name, keep=200):
        """清理指定 RSS 源的历史记录"""
        entries = HistoryManager._read_all_entries()
        
        # 分离当前 RSS 和其他 RSS 的记录
        other_entries = [e for e in entries if e.get('rss_name') != rss_name]
        current_rss_entries = [e for e in entries if e.get('rss_name') == rss_name]
        
        # 保留最新的记录
        current_rss_entries.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        kept_entries = current_rss_entries[:keep]
        
        # 写回文件
        HistoryManager._write_all_entries(other_entries + kept_entries)


# ==================== RSS 解析器 ====================

class RSSParser:
    """RSS 解析器"""
    
    @staticmethod
    def parse_feed(url, include_pat, exclude_pat):
        """解析 RSS 源并返回条目列表"""
        feed = feedparser.parse(url)
        if getattr(feed, 'bozo', False) and not feed.entries:
            raise Exception("无效 RSS 源")
        
        entries = []
        for item in feed.entries:
            entry = RSSParser._parse_item(item, include_pat, exclude_pat)
            if entry:
                entries.append(entry)
        return entries
    
    @staticmethod
    def _parse_item(item, include_pat, exclude_pat):
        """解析单个 RSS 条目"""
        title = item.get('title', '').strip()
        if not title:
            return None
        
        # 提取摘要和内容
        summary = item.get('summary', '')
        content_text = ''
        if hasattr(item, 'content'):
            for c in item.content:
                if c.type in ('text/html', 'xhtml'):
                    content_text = c.value
                    break
        full_desc = f"{summary} {content_text}".strip()
        
        # 查找下载链接
        download_url = RSSParser._extract_download_url(item, full_desc)
        if not download_url:
            return None
        
        # 生成 infohash
        infohash = RSSParser._generate_infohash(download_url)
        if not infohash:
            return None
        
        # 提取图片和时间
        image_url = extract_image_url(item)
        pub_time = item.get('published') or item.get('updated') or ''
        formatted_time = parse_rss_time(pub_time)
        
        # 检查是否匹配过滤器
        should_check = matches_filter(title, full_desc, include_pat, exclude_pat)
        
        return {
            'infohash': infohash,
            'title': title,
            'download_url': download_url,
            'image_url': image_url,
            'summary': full_desc,
            'pub_time': formatted_time,
            'auto_check': should_check,
            'selected': False
        }
    
    @staticmethod
    def _extract_download_url(item, full_desc):
        """提取磁力链接或种子 URL"""
        # 检查 link 字段
        if item.get('link', '').startswith('magnet:'):
            return item.link
        
        # 检查 enclosures
        if hasattr(item, 'enclosures'):
            for enc in item.enclosures:
                href = enc.get('href', '')
                if href.startswith('magnet:'):
                    return href
                elif enc.get('type') == 'application/x-bittorrent':
                    return href
        
        # 检查 links
        if hasattr(item, 'links'):
            for link in item.links:
                href = link.get('href', '')
                if href.startswith('magnet:'):
                    return href
                elif link.get('type') == 'application/x-bittorrent':
                    return href
        
        # 从描述中提取
        extracted = extract_magnet_links(full_desc)
        return extracted[0] if extracted else None
    
    @staticmethod
    def _generate_infohash(url):
        """生成 infohash"""
        if url.startswith('magnet:'):
            return normalize_infohash(url)
        else:
            # 种子 URL 使用 SHA1 生成
            return hashlib.sha1(url.encode()).hexdigest()[:40]


# ==================== 主应用 ====================

class RSSDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("RSS 磁力下载器")
        self.root.geometry("980x780")
        
        self.current_rss_name = None
        self.all_entries = []
        self.check_vars = {}
        self.entry_widgets = {}
        self.photo_images = []
        self.current_page = 0
        self.selected_infohashes = set()
        
        self.create_widgets()
    
    def create_widgets(self):
        """创建 UI 组件"""
        # RSS 输入框
        rss_frame = tk.Frame(self.root)
        rss_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(rss_frame, text="RSS:").pack(side='left')
        self.rss_entry = tk.Entry(rss_frame, width=80)
        self.rss_entry.pack(side='left', fill='x', expand=True)
        tk.Button(rss_frame, text="查询", command=self.fetch_rss).pack(side='right', padx=(5, 0))
        
        # 预设按钮
        preset_frame = tk.Frame(self.root)
        preset_frame.pack(fill='x', padx=10, pady=5)
        for name in PRESET_RSS:
            tk.Button(preset_frame, text=name, 
                     command=lambda n=name: self.load_preset(n)).pack(side='left', padx=5)
        tk.Button(preset_frame, text="清除历史", command=self.clear_history, 
                 fg='red').pack(side='right', padx=(5, 0))
        tk.Button(preset_frame, text="清空", command=self.clear_all).pack(side='right', padx=(5, 0))
        tk.Button(preset_frame, text="全选", command=self.select_all).pack(side='right', padx=(5, 0))
        
        # 过滤器
        filter_frame = tk.Frame(self.root)
        filter_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(filter_frame, text="include:").pack(side='left')
        self.include_entry = tk.Entry(filter_frame, width=40)
        self.include_entry.pack(side='left', padx=(5, 10))
        tk.Label(filter_frame, text="exclude:").pack(side='left')
        self.exclude_entry = tk.Entry(filter_frame, width=40)
        self.exclude_entry.pack(side='left', padx=(5, 10))
        
        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=5)
        
        # 滚动区域
        canvas_frame = tk.Frame(self.root)
        canvas_frame.pack(fill='both', expand=True, padx=10, pady=5)
        self.canvas = tk.Canvas(canvas_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)
        self.scrollable_frame.bind("<Configure>", 
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        self.canvas.bind("<Enter>", lambda e: self.canvas.bind_all("<MouseWheel>", self._on_mousewheel))
        self.canvas.bind("<Leave>", lambda e: self.canvas.unbind_all("<MouseWheel>"))
        
        # 下载按钮
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill='x', padx=10, pady=10)
        tk.Button(btn_frame, text="下载选中项", command=self.download_selected,
                  bg='green', fg='white').pack(side='right')
    
    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(-1 * (event.delta // 120), "units")
    
    def load_preset(self, name):
        """加载预设配置"""
        config = PRESET_RSS[name]
        self.rss_entry.delete(0, tk.END)
        self.rss_entry.insert(0, config["url"])
        self.include_entry.delete(0, tk.END)
        self.include_entry.insert(0, config.get("include", ""))
        self.exclude_entry.delete(0, tk.END)
        self.exclude_entry.insert(0, config.get("exclude", ""))
        self.current_rss_name = name
        self.fetch_rss()
        self.canvas.yview_moveto(0)
    
    def select_all(self):
        """全选当前页"""
        for entry in self.get_current_page_entries():
            self.selected_infohashes.add(entry['infohash'])
        self._render_paginated()
    
    def clear_all(self):
        """清空选择"""
        self.selected_infohashes.clear()
        self._render_paginated()
    
    def get_current_page_entries(self):
        """获取当前页条目"""
        start = self.current_page * PAGE_SIZE
        end = start + PAGE_SIZE
        return self.all_entries[start:end]
    
    def fetch_rss(self):
        """获取 RSS 数据"""
        url = self.rss_entry.get().strip()
        if not url:
            messagebox.showwarning("警告", "请输入 RSS 链接！")
            return
        
        # 确定 RSS 名称
        self.current_rss_name = None
        for name, cfg in PRESET_RSS.items():
            if cfg["url"] == url:
                self.current_rss_name = name
                break
        if not self.current_rss_name:
            self.current_rss_name = "custom"
        
        # 清空界面
        self._clear_ui()
        
        include_pat = self.include_entry.get()
        exclude_pat = self.exclude_entry.get()
        Thread(target=self._fetch_rss_thread, args=(url, include_pat, exclude_pat), daemon=True).start()
    
    def _clear_ui(self):
        """清空 UI"""
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.check_vars.clear()
        self.entry_widgets.clear()
        self.photo_images.clear()
        self.all_entries = []
    
    def _fetch_rss_thread(self, url, include_pat, exclude_pat):
        """后台线程：获取 RSS 数据"""
        try:
            # 解析 RSS
            new_entries = RSSParser.parse_feed(url, include_pat, exclude_pat)
            
            # 保存到历史（会保留已下载状态）
            HistoryManager.save(new_entries, self.current_rss_name)
            
            # 加载历史记录（此时已包含最新数据和已下载状态）
            history_dict = HistoryManager.load_by_rss_name(self.current_rss_name)
            
            # 直接使用历史记录中的数据
            all_entries = list(history_dict.values())
         # 按类型分组并按日期倒序排列
            downloaded = sorted([e for e in all_entries if e.get('selected')], 
                              key=lambda e: e.get('pub_time', ''), reverse=True)
            checked = sorted([e for e in all_entries if not e.get('selected') and e.get('auto_check')], 
                           key=lambda e: e.get('pub_time', ''), reverse=True)
            unchecked = sorted([e for e in all_entries if not e.get('selected') and not e.get('auto_check')], 
                             key=lambda e: e.get('pub_time', ''), reverse=True)
            
            self.all_entries = checked + unchecked + downloaded
            
            # 初始化选中状态：自动选中匹配过滤器且未下载的条目
            self.selected_infohashes = {
                e['infohash'] for e in self.all_entries 
                if e.get('auto_check', False) and not e.get('selected', False)
            }
            
            self.current_page = 0
            self.root.after(0, self._render_paginated)
            self.root.after(0, lambda: self.canvas.yview_moveto(0))
        except Exception as e:
            error_msg = f"解析失败:\n{str(e)}"
            self.root.after(0, lambda msg=error_msg: messagebox.showerror("错误", msg))
    
    def _render_paginated(self):
        """渲染分页内容"""
        # 清空当前页
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.check_vars.clear()
        self.entry_widgets.clear()
        self.photo_images.clear()
        
        page_entries = self.get_current_page_entries()
        
        if not page_entries:
            tk.Label(self.scrollable_frame, text="暂无数据", fg='gray').pack(pady=20)
            self._show_pagination_controls(0)
            return
        
        # 创建条目
        for entry in page_entries:
            self._create_entry_widget(entry)
        
        # 显示分页控件
        total_pages = (len(self.all_entries) + PAGE_SIZE - 1) // PAGE_SIZE
        self._show_pagination_controls(total_pages)
    
    def _show_pagination_controls(self, total_pages):
        """显示分页控件"""
        # 删除旧的分页控件
        for child in self.root.winfo_children():
            if getattr(child, '_is_pagination', False):
                child.destroy()
        
        if total_pages <= 1:
            return
        
        frame = tk.Frame(self.root)
        frame._is_pagination = True
        frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(frame, text=f"第 {self.current_page + 1} / {total_pages} 页").pack(side='left')
        
        tk.Button(frame, text="← 上一页",
                  command=lambda: self._go_to_page(self.current_page - 1),
                  state='normal' if self.current_page > 0 else 'disabled').pack(side='left', padx=5)
        
        tk.Button(frame, text="下一页 →",
                  command=lambda: self._go_to_page(self.current_page + 1),
                  state='normal' if self.current_page < total_pages - 1 else 'disabled').pack(side='left', padx=5)
    
    def _go_to_page(self, page):
        """跳转到指定页"""
        self.current_page = page
        self._render_paginated()
        self.canvas.yview_moveto(0)
    
    def _create_entry_widget(self, entry):
        """创建单个条目 UI"""
        infohash = entry['infohash']
        is_selected = entry.get('selected', False)
        
        # 已下载的用绿色背景
        bg_color = '#d4edda' if is_selected else 'white'
        
        frame = tk.Frame(self.scrollable_frame, relief='groove', bd=1, padx=5, pady=5, bg=bg_color)
        frame.pack(fill='x', pady=4, padx=2)
        
        # 复选框：只用 selected_infohashes
        is_checked = infohash in self.selected_infohashes
        var = tk.BooleanVar(value=is_checked)
        cb = tk.Checkbutton(frame, variable=var, bg=bg_color,
                           command=lambda: self._on_check_change(infohash, var.get()))
        cb.pack(side='left', padx=(0, 10))
        
        # 可点击区域
        clickable_area = tk.Frame(frame, bg=bg_color)
        clickable_area.pack(side='left', fill='both', expand=True)
        
        # 按钮区域
        btn_frame = tk.Frame(clickable_area, bg=bg_color)
        btn_frame.pack(anchor='ne')
        tk.Button(btn_frame, text="🗑️", width=2, 
                 command=lambda: self._remove_entry(infohash)).pack(side='left')
        if entry.get('image_url'):
            tk.Button(btn_frame, text="🔄", width=2,
                     command=lambda: self._retry_image(infohash)).pack(side='left')
        
        # 图片
        if entry.get('image_url'):
            img_container = tk.Frame(clickable_area, width=IMAGE_WIDTH, height=IMAGE_HEIGHT, bg='#eee')
            img_container.pack_propagate(False)
            img_container.pack(side='left', padx=(0, 10), pady=2)
            img_label = tk.Label(img_container, bg='#eee', text="加载中…", fg='gray')
            img_label.pack(expand=True)
            Thread(target=self._load_image, args=(entry['image_url'], img_label), daemon=True).start()
        
        # 文本区域
        text_frame = tk.Frame(clickable_area, bg=bg_color)
        text_frame.pack(side='left', fill='both', expand=True, padx=(0, 10))
        
        tk.Label(text_frame, text=entry['title'], font=('Microsoft YaHei', 12, 'bold'),
                wraplength=TEXT_WRAP_LENGTH, justify='left', anchor='w', bg=bg_color).pack(anchor='w', pady=(0, 2))
        
        tk.Label(text_frame, text=entry.get('pub_time', '未知时间'), font=('Microsoft YaHei', 10),
                fg='gray50', wraplength=TEXT_WRAP_LENGTH, justify='left', anchor='w', bg=bg_color).pack(anchor='w', pady=(0, 3))
        
        summary_text = entry.get('summary', '').strip()
        if summary_text:
            clean_summary = re.sub(r'<[^>]+>', '', summary_text)
            tk.Label(text_frame, text=clean_summary, font=('Microsoft YaHei', 11),
                    fg='gray40', wraplength=TEXT_WRAP_LENGTH, justify='left', anchor='w', bg=bg_color).pack(anchor='w')
        
        # 点击切换选中状态
        def toggle(event):
            # 检查是否点击了按钮区域
            widget = event.widget
            current = widget
            while current:
                if current == btn_frame:
                    return
                current = current.master
            new_val = not var.get()
            var.set(new_val)
            self._on_check_change(infohash, new_val)
        
        clickable_area.bind("<Button-1>", toggle)
        for child in clickable_area.winfo_children():
            child.bind("<Button-1>", toggle)
            if hasattr(child, 'winfo_children'):
                for grand in child.winfo_children():
                    grand.bind("<Button-1>", toggle)
        
        self.check_vars[infohash] = var
        self.entry_widgets[infohash] = frame
    
    def _on_check_change(self, infohash, is_checked):
        """复选框状态改变"""
        if is_checked:
            self.selected_infohashes.add(infohash)
        else:
            self.selected_infohashes.discard(infohash)
    
    def _load_image(self, url, label):
        """加载图片"""
        try:
            resp = requests.get(url, timeout=IMAGE_TIMEOUT)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            img.thumbnail((IMAGE_WIDTH, IMAGE_HEIGHT), Image.LANCZOS)
            tk_img = ImageTk.PhotoImage(img)
            self.root.after(0, lambda: self._set_image(label, tk_img))
        except Exception:
            self.root.after(0, lambda: label.config(text="加载失败", fg='red'))
    
    def _set_image(self, label, tk_img):
        """设置图片"""
        label.config(image=tk_img, text="")
        self.photo_images.append(tk_img)
    
    def _retry_image(self, infohash):
        """重试加载图片"""
        entry = next((e for e in self.all_entries if e['infohash'] == infohash), None)
        if not entry or not entry.get('image_url'):
            return
        
        frame = self.entry_widgets.get(infohash)
        if not frame:
            return
        
        # 查找图片标签并重试
        for widget in frame.winfo_children():
            if isinstance(widget, tk.Frame):
                for child in widget.winfo_children():
                    if isinstance(child, tk.Frame) and child.winfo_reqwidth() == IMAGE_WIDTH:
                        for lbl in child.winfo_children():
                            if isinstance(lbl, tk.Label):
                                lbl.config(text="重试中…", image='', fg='gray')
                                Thread(target=self._load_image, args=(entry['image_url'], lbl), daemon=True).start()
                                return
    
    def _remove_entry(self, infohash):
        """删除条目"""
        if infohash in self.entry_widgets:
            self.entry_widgets[infohash].destroy()
            del self.entry_widgets[infohash]
        if infohash in self.check_vars:
            del self.check_vars[infohash]
        self.selected_infohashes.discard(infohash)
    
    def download_selected(self):
        """下载选中项"""
        if not self.selected_infohashes:
            messagebox.showwarning("提示", "请先选择要下载的条目！")
            return
        
        selected_urls = []
        selected_hashes = []
        for infohash in self.selected_infohashes:
            entry = next((e for e in self.all_entries if e['infohash'] == infohash), None)
            if entry and entry.get('download_url'):
                selected_urls.append(entry['download_url'])
                selected_hashes.append(infohash)
        
        if not selected_urls:
            messagebox.showwarning("提示", "没有可下载的条目！")
            return
        
        Thread(target=self._download_links, args=(selected_urls, selected_hashes), daemon=True).start()
    
    def clear_history(self):
        """清理历史记录"""
        if not self.current_rss_name:
            messagebox.showwarning("警告", "请先选择一个 RSS 源！")
            return
        if messagebox.askyesno("确认", f"将保留最新的 {MAX_DISPLAY_ENTRIES} 条记录，删除更早的历史，是否继续？"):
            Thread(target=lambda: HistoryManager.clear_for_rss(self.current_rss_name, MAX_DISPLAY_ENTRIES), 
                  daemon=True).start()
            messagebox.showinfo("提示", "历史已清理（后台执行）")
    
    def _download_links(self, urls, infohashes):
        """后台线程：下载链接"""
        try:
            session = requests.Session()
            login_data = {"username": USERNAME, "password": PASSWORD}
            session.post(QB_LOGIN_URL, data=login_data, timeout=10)
            
            for url in urls:
                body = {
                    "urls": url,
                    "savepath": '/Volumes/Storage/download/A',
                    "rename": 'a',
                    "autoTMM": "false"
                }
                session.post(QB_ADD_TORRENT_URL, data=body, timeout=10)
            
            # 标记为已下载
            HistoryManager.mark_as_selected(self.current_rss_name, infohashes)
            
            # 更新 UI
            for infohash in infohashes:
                for entry in self.all_entries:
                    if entry['infohash'] == infohash:
                        entry['selected'] = True
            
            self.selected_infohashes.clear()
            success_msg = f"{len(urls)} 个任务已提交！"
            self.root.after(0, lambda msg=success_msg: messagebox.showinfo("成功", msg))
            self.root.after(0, self._render_paginated)
        except Exception as e:
            error_msg = f"下载失败:\n{str(e)}"
            self.root.after(0, lambda msg=error_msg: messagebox.showerror("错误", msg))


if __name__ == "__main__":
    root = tk.Tk()
    app = RSSDownloaderApp(root)
    root.mainloop()

