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
IMAGE_WIDTH = 120
IMAGE_HEIGHT = 120
IMAGE_TIMEOUT = 15
PAGE_SIZE = 50
MAX_DISPLAY_ENTRIES = 200
HISTORY_FILE = "rss.log"

# UI配置
COLORS = {
    'bg': '#f5f5f5',
    'selected_bg': '#e8f4f8',
    'downloaded_bg': '#d4edda',
    'hover_bg': '#f0f0f0',
    'border': '#ddd',
    'text_primary': '#212529',
    'text_secondary': '#6c757d',
    'text_tertiary': '#adb5bd'
}

FONTS = {
    'title': ('Microsoft YaHei', 11, 'bold'),
    'author': ('Microsoft YaHei', 9),
    'content': ('Microsoft YaHei', 8),
    'time': ('Microsoft YaHei', 9),
    'ui': ('Microsoft YaHei', 10)
}


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


def matches_filter(title, author, summary, include_pat, exclude_pat):
    """检查标题、作者和摘要是否匹配过滤器"""
    full_text = f"{title or ''} {author or ''} {summary or ''}"
    
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


def should_delete(title, author, summary, delete_pat):
    """检查是否应该删除（匹配delete过滤器）"""
    if not delete_pat.strip():
        return False
    
    full_text = f"{title or ''} {author or ''} {summary or ''}"
    try:
        return bool(re.search(delete_pat, full_text, re.IGNORECASE))
    except re.error:
        return False


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
        
        # 过滤：排除已删除的条目
        history = {
            e['infohash']: e 
            for e in entries 
            if e.get('rss_name') == rss_name 
            and e.get('infohash') 
            and not e.get('deleted', False)
        }
        entries_list = sorted(history.values(), key=lambda x: x.get('timestamp', ''), reverse=True)
        return {e['infohash']: e for e in entries_list[:MAX_DISPLAY_ENTRIES]}
    
    @staticmethod
    def save(new_entries, rss_name):
        """保存新条目到历史记录"""
        # 读取现有记录
        entries = HistoryManager._read_all_entries()
        existing = {(e.get('rss_name'), e.get('infohash')): e for e in entries if e.get('rss_name') and e.get('infohash')}
        
        # 合并新条目：用最新数据覆盖，但保留 selected 和 deleted 状态
        now_iso = datetime.now().isoformat()
        for e in new_entries:
            key = (rss_name, e['infohash'])
            old_entry = existing.get(key, {})
            old_selected = old_entry.get('selected', False)
            old_deleted = old_entry.get('deleted', False)
            
            # 用新数据覆盖
            out_entry = e.copy()
            out_entry['rss_name'] = rss_name
            out_entry['timestamp'] = now_iso
            out_entry['selected'] = old_selected
            out_entry['deleted'] = old_deleted or e.get('deleted', False)  # 如果新条目标记为删除，也保留
            existing[key] = out_entry
        
        # 按时间排序后写入
        all_entries = sorted(existing.values(), key=lambda x: x.get('timestamp', ''))
        HistoryManager._write_all_entries(all_entries)
    
    @staticmethod
    def mark_as_deleted(rss_name, infohashes):
        """标记条目为已删除"""
        entries = HistoryManager._read_all_entries()
        
        # 更新删除状态
        infohash_set = set(infohashes)
        for entry in entries:
            if entry.get('rss_name') == rss_name and entry.get('infohash') in infohash_set:
                entry['deleted'] = True
        
        HistoryManager._write_all_entries(entries)
    
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
    """RSS 解析器 - 支持 RSS 2.0 和 Atom"""
    
    @staticmethod
    def parse_feed(url, include_pat, exclude_pat, delete_pat):
        """解析 RSS 源并返回条目列表"""
        feed = feedparser.parse(url)
        if getattr(feed, 'bozo', False) and not feed.entries:
            raise Exception("无效 RSS 源")
        
        entries = []
        for item in feed.entries:
            entry = RSSParser._parse_item(item, include_pat, exclude_pat, delete_pat)
            if entry:
                entries.append(entry)
        return entries
    
    @staticmethod
    def _parse_item(item, include_pat, exclude_pat, delete_pat):
        """解析单个 RSS 条目"""
        title = item.get('title', '').strip()
        if not title:
            return None
        
        # 提取作者 - 支持多种格式
        author = RSSParser._extract_author(item)
        
        # 提取摘要和内容
        summary = item.get('summary', '')
        content_text = ''
        if hasattr(item, 'content'):
            for c in item.content:
                if c.type in ('text/html', 'xhtml', 'application/xhtml+xml'):
                    content_text = c.value
                    break
        
        # 如果没有summary但有content，使用content
        if not summary and content_text:
            summary = content_text
        
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
        image_url = RSSParser._extract_image_url(item, full_desc)
        pub_time = item.get('published') or item.get('updated') or item.get('pubDate') or ''
        formatted_time = parse_rss_time(pub_time)
        
        # 检查是否匹配过滤器
        should_check = matches_filter(title, author, full_desc, include_pat, exclude_pat)
        
        # 检查是否应该删除
        is_deleted = should_delete(title, author, full_desc, delete_pat)
        
        return {
            'infohash': infohash,
            'title': title,
            'author': author,
            'download_url': download_url,
            'image_url': image_url,
            'summary': full_desc,
            'pub_time': formatted_time,
            'auto_check': should_check,
            'selected': False,
            'deleted': is_deleted
        }
    
    @staticmethod
    def _extract_author(item):
        """提取作者信息 - 支持RSS2.0和Atom"""
        # Atom: author.name 或 author_detail.name
        if hasattr(item, 'author_detail'):
            name = item.author_detail.get('name', '')
            if name:
                return name
        
        # RSS 2.0: author 字段
        if hasattr(item, 'author') and item.author:
            # 有些RSS的author是邮箱格式: "email@example.com (Name)"
            match = re.search(r'\(([^)]+)\)', item.author)
            if match:
                return match.group(1)
            # 或者直接是名字
            if '@' not in item.author:
                return item.author
        
        # Dublin Core: dc:creator
        if hasattr(item, 'dc_creator') and item.dc_creator:
            return item.dc_creator
        
        # 备选：从tags中查找author
        if hasattr(item, 'tags'):
            for tag in item.tags:
                if tag.get('scheme') == 'http://purl.org/dc/elements/1.1/creator':
                    return tag.get('term', '')
        
        return ''
    
    @staticmethod
    def _extract_image_url(item, full_desc):
        """提取图片URL - 支持多种格式"""# 1. Atom: media:thumbnail
        if hasattr(item, 'media_thumbnail') and item.media_thumbnail:
            for thumb in item.media_thumbnail:
                url = thumb.get('url')
                if url:
                    return url
        
        # 2. Atom: media:content
        if hasattr(item, 'media_content'):
            for media in item.media_content:
                if media.get('type', '').startswith('image/'):
                    url = media.get('url')
                    if url:
                        return url
        
        # 3. RSS 2.0: enclosure (type="image/*")
        if hasattr(item, 'enclosures'):
            for enc in item.enclosures:
                if enc.get('type', '').startswith('image/'):
                    return enc.get('href', '')
        
        # 4. content中的图片
        if hasattr(item, 'content'):
            for c in item.content:
                if c.type in ('text/html', 'xhtml', 'application/xhtml+xml'):
                    img = extract_image_url_from_html(c.value)
                    if img:
                        return img
        
        # 5. summary/description中的图片
        for field in ['summary', 'description']:
            value = getattr(item, field, '')
            if value:
                img = extract_image_url_from_html(value)
                if img:
                    return img
        
        return None
    
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
        self.root.geometry("1100x800")
        self.root.configure(bg=COLORS['bg'])
        
        self.current_rss_name = None
        self.all_entries = []
        self.check_vars = {}
        self.entry_widgets = {}
        self.photo_images = []
        self.current_page = 0
        self.selected_infohashes = set()
        self.status_message = ""
        
        self.create_widgets()
    
    def create_widgets(self):
        """创建 UI 组件"""
        # 顶部控制面板
        top_panel = tk.Frame(self.root, bg='white', relief='flat', bd=0)
        top_panel.pack(fill='x', padx=0, pady=0)
        
        # RSS 输入区域
        rss_frame = tk.Frame(top_panel, bg='white', pady=10, padx=15)
        rss_frame.pack(fill='x')
        
        tk.Label(rss_frame, text="RSS 源:", font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left', padx=(0, 8))
        self.rss_entry = tk.Entry(rss_frame, font=FONTS['ui'], relief='solid', bd=1)
        self.rss_entry.pack(side='left', fill='x', expand=True, ipady=4)
        
        query_btn = tk.Button(rss_frame, text="查询", command=self.fetch_rss, 
                             font=FONTS['ui'], bg='#007bff', fg='white', 
                             relief='flat', padx=20, cursor='hand2')
        query_btn.pack(side='left', padx=(10, 0))
        
        # 预设按钮区域
        preset_frame = tk.Frame(top_panel, bg='white', pady=5, padx=15)
        preset_frame.pack(fill='x')
        
        tk.Label(preset_frame, text="预设:", font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left', padx=(0, 8))
        
        for name in PRESET_RSS:
            btn = tk.Button(preset_frame, text=name, 
                           command=lambda n=name: self.load_preset(n),
                           font=FONTS['ui'], bg='#6c757d', fg='white',
                           relief='flat', padx=15, cursor='hand2')
            btn.pack(side='left', padx=3)
        
        # 右侧操作按钮
        tk.Button(preset_frame, text="清除历史", command=self.clear_history,
                 font=FONTS['ui'], bg='#dc3545', fg='white',
                 relief='flat', padx=15, cursor='hand2').pack(side='right', padx=3)
        tk.Button(preset_frame, text="清空", command=self.clear_all,
                 font=FONTS['ui'], bg='#ffc107', fg='white',
                 relief='flat', padx=15, cursor='hand2').pack(side='right', padx=3)
        tk.Button(preset_frame, text="全选", command=self.select_all,
                 font=FONTS['ui'], bg='#28a745', fg='white',
                 relief='flat', padx=15, cursor='hand2').pack(side='right', padx=3)
        tk.Button(preset_frame, text="更新", command=self.update_rss,
                 font=FONTS['ui'], bg='#007bff', fg='white',
                 relief='flat', padx=15, cursor='hand2').pack(side='right', padx=3)
        
        # 过滤器区域
        filter_frame = tk.Frame(top_panel, bg='white', pady=10, padx=15)
        filter_frame.pack(fill='x')
        
        tk.Label(filter_frame, text="包含:", font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left', padx=(0, 5))
        self.include_entry = tk.Entry(filter_frame, width=25, font=FONTS['ui'], relief='solid', bd=1)
        self.include_entry.pack(side='left', padx=(0, 15), ipady=3)
        
        tk.Label(filter_frame, text="排除:", font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left', padx=(0, 5))
        self.exclude_entry = tk.Entry(filter_frame, width=25, font=FONTS['ui'], relief='solid', bd=1)
        self.exclude_entry.pack(side='left', padx=(0, 15), ipady=3)
        
        tk.Label(filter_frame, text="删除:", font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left', padx=(0, 5))
        self.delete_entry = tk.Entry(filter_frame, width=25, font=FONTS['ui'], relief='solid', bd=1)
        self.delete_entry.pack(side='left', ipady=3)
        
        # 分隔线
        ttk.Separator(self.root, orient='horizontal').pack(fill='x', pady=0)
        
        # 滚动区域
        canvas_frame = tk.Frame(self.root, bg=COLORS['bg'])
        canvas_frame.pack(fill='both', expand=True, padx=0, pady=0)
        
        self.canvas = tk.Canvas(canvas_frame, bg=COLORS['bg'], highlightthickness=0)
        scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas, bg=COLORS['bg'])
        self.scrollable_frame.bind("<Configure>", 
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        self.canvas.bind("<Enter>", lambda e: self.canvas.bind_all("<MouseWheel>", self._on_mousewheel))
        self.canvas.bind("<Leave>", lambda e: self.canvas.unbind_all("<MouseWheel>"))
        
        # 底部状态栏和按钮
        bottom_frame = tk.Frame(self.root, bg='white', relief='flat', bd=0)
        bottom_frame.pack(fill='x', padx=0, pady=0)
        
        ttk.Separator(bottom_frame, orient='horizontal').pack(fill='x')
        
        btn_container = tk.Frame(bottom_frame, bg='white', pady=10, padx=15)
        btn_container.pack(fill='x')
        
        # 状态信息标签
        self.status_label = tk.Label(btn_container, text="", font=FONTS['ui'], 
                                     bg='white', fg=COLORS['text_secondary'])
        self.status_label.pack(side='left')
        
        # 下载按钮
        download_btn = tk.Button(btn_container, text="下载选中项", command=self.download_selected,
                                font=FONTS['ui'], bg='#28a745', fg='white',
                                relief='flat', padx=30, pady=5, cursor='hand2')
        download_btn.pack(side='right')
    
    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(-1 * (event.delta // 120), "units")
    
    def update_status(self, message, error=False):
        """更新状态栏消息"""
        self.status_message = message
        color = '#dc3545' if error else COLORS['text_secondary']
        self.status_label.config(text=message, fg=color)
    
    def load_preset(self, name):
        """加载预设配置 - 只从历史记录加载"""
        config = PRESET_RSS[name]
        self.rss_entry.delete(0, tk.END)
        self.rss_entry.insert(0, config["url"])
        self.include_entry.delete(0, tk.END)
        self.include_entry.insert(0, config.get("include", ""))
        self.exclude_entry.delete(0, tk.END)
        self.exclude_entry.insert(0, config.get("exclude", ""))
        self.delete_entry.delete(0, tk.END)
        self.delete_entry.insert(0, config.get("delete", ""))
        self.current_rss_name = name
        self.load_from_history()
        self.canvas.yview_moveto(0)
    
    def load_from_history(self):
        """从历史记录加载数据"""
        if not self.current_rss_name:
            self.update_status("请先选择一个 RSS 源！", error=True)
            return
        
        self._clear_ui()
        self.update_status("正在加载历史记录...")
        
        # 加载历史记录
        history_dict = HistoryManager.load_by_rss_name(self.current_rss_name)
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
        self._render_paginated()
        self.canvas.yview_moveto(0)
        self.update_status(f"加载完成，共 {len(self.all_entries)} 条历史记录")
    
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
    
    def update_rss(self):
        """更新 RSS 数据 - 连接RSS源获取最新数据"""
        if not self.current_rss_name:
            self.update_status("请先选择一个 RSS 源！", error=True)
            return
        
        url = self.rss_entry.get().strip()
        if not url:
            self.update_status("请输入 RSS 链接！", error=True)
            return
        
        self.update_status("正在连接RSS源更新数据...")
        include_pat = self.include_entry.get()
        exclude_pat = self.exclude_entry.get()
        delete_pat = self.delete_entry.get()
        Thread(target=self._fetch_rss_thread, args=(url, include_pat, exclude_pat, delete_pat), daemon=True).start()
    
    def fetch_rss(self):
        """获取 RSS 数据（用于查询按钮）"""
        url = self.rss_entry.get().strip()
        if not url:
            self.update_status("请输入 RSS 链接！", error=True)
            return
        
        # 确定 RSS 名称
        self.current_rss_name = None
        for name, cfg in PRESET_RSS.items():
            if cfg["url"] == url:
                self.current_rss_name = name
                break
        if not self.current_rss_name:
            self.current_rss_name = "custom"
        
        # 直接从历史记录加载
        self.load_from_history()
    
    def _clear_ui(self):
        """清空 UI"""
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.check_vars.clear()
        self.entry_widgets.clear()
        self.photo_images.clear()
        self.all_entries = []
    
    def _fetch_rss_thread(self, url, include_pat, exclude_pat, delete_pat):
        """后台线程：获取 RSS 数据"""
        try:
            # 解析 RSS
            new_entries = RSSParser.parse_feed(url, include_pat, exclude_pat, delete_pat)
            
            # 保存到历史（会保留已下载和已删除状态）
            HistoryManager.save(new_entries, self.current_rss_name)
            
            # 加载历史记录（此时已包含最新数据，已删除的条目会被过滤掉）
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
            self.root.after(0, lambda: self.update_status(f"更新完成，共 {len(self.all_entries)} 条记录"))
        except Exception as e:
            error_msg = f"更新失败: {str(e)}"
            self.root.after(0, lambda: self.update_status(error_msg, error=True))
    
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
            empty_label = tk.Label(self.scrollable_frame, text="暂无数据", 
                                  font=FONTS['ui'], fg=COLORS['text_tertiary'],
                                  bg=COLORS['bg'])
            empty_label.pack(pady=50)
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
        # 如果只有一页或没有数据，不显示分页控件
        if total_pages <= 1:
            # 删除旧的分页控件
            for child in self.root.winfo_children():
                if getattr(child, '_is_pagination', False):
                    child.destroy()
            return
        
        # 删除旧的分页控件
        for child in self.root.winfo_children():
            if getattr(child, '_is_pagination', False):
                child.destroy()
        
        # 创建新的分页控件
        frame = tk.Frame(self.root, bg='white', relief='flat')
        frame._is_pagination = True
        
        ttk.Separator(frame, orient='horizontal').pack(fill='x')
        
        page_container = tk.Frame(frame, bg='white', pady=8)
        page_container.pack(fill='x')
        
        tk.Label(page_container, text=f"第 {self.current_page + 1} / {total_pages} 页", 
                font=FONTS['ui'], bg='white', fg=COLORS['text_primary']).pack(side='left')
        
        btn_style = {'font': FONTS['ui'], 'bg': '#6c757d', 'fg': 'white', 
                    'relief': 'flat', 'padx': 15, 'cursor': 'hand2'}
        
        tk.Button(page_container, text="← 上一页",
                  command=lambda: self._go_to_page(self.current_page - 1),
                  state='normal' if self.current_page > 0 else 'disabled',
                  **btn_style).pack(side='left', padx=5)
        
        tk.Button(page_container, text="下一页 →",
                  command=lambda: self._go_to_page(self.current_page + 1),
                  state='normal' if self.current_page < total_pages - 1 else 'disabled',
                  **btn_style).pack(side='left', padx=5)
        
        # 确保分页控件在底部按钮栏之前显示
        # 找到底部按钮栏
        bottom_frame = None
        for child in self.root.winfo_children():
            if isinstance(child, tk.Frame) and not getattr(child, '_is_pagination', False):
                # 检查是否有下载按钮
                for subchild in child.winfo_children():
                    if isinstance(subchild, tk.Frame):
                        for btn in subchild.winfo_children():
                            if isinstance(btn, tk.Button) and "下载" in btn.cget('text'):
                                bottom_frame = child
                                break
        
        if bottom_frame:
            frame.pack(fill='x', padx=15, pady=0, before=bottom_frame)
        else:
            frame.pack(fill='x', padx=15, pady=0)

    
    def _go_to_page(self, page):
        """跳转到指定页"""
        self.current_page = page
        self._render_paginated()
        self.canvas.yview_moveto(0)
    
    def _create_entry_widget(self, entry):
        """创建单个条目 UI - 新设计"""
        infohash = entry['infohash']
        is_selected = entry.get('selected', False)
        is_checked = infohash in self.selected_infohashes
        
        # 背景色：已下载 > 已选中 > 默认
        if is_selected:
            bg_color = COLORS['downloaded_bg']
        elif is_checked:
            bg_color = COLORS['selected_bg']
        else:
            bg_color = 'white'
        
        # 主容器
        frame = tk.Frame(self.scrollable_frame, bg=bg_color, relief='solid', bd=1)
        frame.pack(fill='x', pady=3, padx=10)
        
        # 内边距容器
        inner_frame = tk.Frame(frame, bg=bg_color, padx=10, pady=10)
        inner_frame.pack(fill='x')
        
        # 复选框
        var = tk.BooleanVar(value=is_checked)
        cb = tk.Checkbutton(inner_frame, variable=var, bg=bg_color,
                           command=lambda: self._on_check_change(infohash, var.get()),
                           cursor='hand2')
        cb.pack(side='left', padx=(0, 10))
        
        # 时间列
        time_frame = tk.Frame(inner_frame, bg=bg_color, width=90)
        time_frame.pack_propagate(False)
        time_frame.pack(side='left', padx=(0, 10), fill='y')
        
        time_label = tk.Label(time_frame, text=entry.get('pub_time', '未知时间'),
                             font=FONTS['time'], fg=COLORS['text_secondary'],
                             bg=bg_color, justify='center', wraplength=85)
        time_label.pack(expand=True)
        
     # 图片（如果有）
        if entry.get('image_url'):
            img_container = tk.Frame(inner_frame, width=IMAGE_WIDTH, height=IMAGE_HEIGHT, 
                                    bg='#f0f0f0', relief='flat', bd=1)
            img_container.pack_propagate(False)
            img_container.pack(side='left', padx=(0, 15))
            
            img_label = tk.Label(img_container, bg='#f0f0f0', text="加载中…", 
                               fg=COLORS['text_tertiary'], font=('Arial', 8))
            img_label.pack(expand=True)
            Thread(target=self._load_image, args=(entry['image_url'], img_label), daemon=True).start()
        
        # 内容区域（标题 + 作者 + 摘要）
        content_frame = tk.Frame(inner_frame, bg=bg_color)
        content_frame.pack(side='left', fill='both', expand=True)
        
        # 删除按钮
        delete_btn = tk.Button(inner_frame, text="🗑️", 
                              command=lambda: self._delete_entry_permanently(infohash),
                              font=('Arial', 14), bg='#dc3545', fg='white',
                              relief='flat', padx=8, pady=4, cursor='hand2',
                              width=3)
        delete_btn.pack(side='right', padx=(10, 0))
        
        # 标题 - 3行，黑体
        title_text = tk.Text(content_frame, font=FONTS['title'], fg=COLORS['text_primary'],
                            wrap='word', height=3, bg=bg_color, relief='flat',
                            borderwidth=0, highlightthickness=0, cursor='xterm')
        title_text.insert('1.0', entry['title'])
        title_text.config(state='disabled')
        title_text.pack(anchor='w', fill='x')
        
        # 作者 - 1行
        if entry.get('author'):
            author_text = tk.Text(content_frame, font=FONTS['author'], fg=COLORS['text_secondary'],
                                wrap='word', height=1, bg=bg_color, relief='flat',
                                borderwidth=0, highlightthickness=0, cursor='xterm')
            author_text.insert('1.0', f"👤 {entry['author']}")
            author_text.config(state='disabled')
            author_text.pack(anchor='w', fill='x', pady=(2, 2))
        
        # 摘要 - 3行，小字体
        summary_text = entry.get('summary', '').strip()
        if summary_text:
            clean_summary = re.sub(r'<[^>]+>', '', summary_text)
            # 截取前200个字符避免过长
            if len(clean_summary) > 200:
                clean_summary = clean_summary[:200] + '...'
            
            summary_text_widget = tk.Text(content_frame, font=FONTS['content'], 
                                         fg=COLORS['text_tertiary'],
                                         wrap='word', height=3, bg=bg_color, relief='flat',
                                         borderwidth=0, highlightthickness=0, cursor='xterm')
            summary_text_widget.insert('1.0', clean_summary)
            summary_text_widget.config(state='disabled')
            summary_text_widget.pack(anchor='w', fill='x', pady=(2, 0))
        
        # 右键菜单
        context_menu = Menu(frame, tearoff=0)
        context_menu.add_command(label="删除此条目", 
                                command=lambda: self._remove_entry(infohash))
        if entry.get('image_url'):
            context_menu.add_command(label="重新加载图片", 
                                    command=lambda: self._retry_image(infohash))
        context_menu.add_command(label="复制标题", 
                                command=lambda: self.root.clipboard_clear() or self.root.clipboard_append(entry['title']))
        
        # 绑定右键菜单
        def show_context_menu(event):
            context_menu.post(event.x_root, event.y_root)
        
        frame.bind("<Button-3>", show_context_menu)
        inner_frame.bind("<Button-3>", show_context_menu)
        
        # 点击切换选中状态（排除Text组件）
        def toggle(event):
            widget = event.widget
            if isinstance(widget, tk.Text):
                return
            new_val = not var.get()
            var.set(new_val)
            self._on_check_change(infohash, new_val)
        
        frame.bind("<Button-1>", toggle)
        inner_frame.bind("<Button-1>", toggle)
        time_frame.bind("<Button-1>", toggle)
        time_label.bind("<Button-1>", toggle)
        if entry.get('image_url'):
            img_container.bind("<Button-1>", toggle)
            img_label.bind("<Button-1>", toggle)
        content_frame.bind("<Button-1>", toggle)
        
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
            self.root.after(0, lambda: self._set_image_error(label))
    
    def _set_image(self, label, tk_img):
        """设置图片"""
        try:
            # 检查label是否还存在
            if label.winfo_exists():
                label.config(image=tk_img, text="")
                self.photo_images.append(tk_img)
        except tk.TclError:
            # 控件已被销毁，忽略
            pass
    
    def _set_image_error(self, label):
        """设置图片加载失败"""
        try:
            if label.winfo_exists():
                label.config(text="加载失败", fg='red')
        except tk.TclError:
            pass
    
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
        """删除条目（仅UI）"""
        if infohash in self.entry_widgets:
            self.entry_widgets[infohash].destroy()
            del self.entry_widgets[infohash]
        if infohash in self.check_vars:
            del self.check_vars[infohash]
        self.selected_infohashes.discard(infohash)
    
    def _delete_entry_permanently(self, infohash):
        """永久删除条目（UI + 历史记录）"""
        # 从UI中删除
        self._remove_entry(infohash)
        
        # 从all_entries中删除
        self.all_entries = [e for e in self.all_entries if e['infohash'] != infohash]
        
        # 在历史记录中标记为删除
        HistoryManager.mark_as_deleted(self.current_rss_name, [infohash])
        
        # 更新状态
        self.update_status(f"✓ 已删除条目")
    
    
    def download_selected(self):
        """下载选中项"""
        if not self.selected_infohashes:
            self.update_status("请先选择要下载的条目！", error=True)
            return
        
        selected_urls = []
        selected_hashes = []
        for infohash in self.selected_infohashes:
            entry = next((e for e in self.all_entries if e['infohash'] == infohash), None)
            if entry and entry.get('download_url'):
                selected_urls.append(entry['download_url'])
                selected_hashes.append(infohash)
        
        if not selected_urls:
            self.update_status("没有可下载的条目！", error=True)
            return
        
        self.update_status(f"正在提交 {len(selected_urls)} 个下载任务...")
        Thread(target=self._download_links, args=(selected_urls, selected_hashes), daemon=True).start()
    
    def clear_history(self):
        """清理所有RSS源的历史记录"""
        if messagebox.askyesno("确认", f"将清理所有RSS源的历史记录，每个源仅保留最新的 {MAX_DISPLAY_ENTRIES} 条，是否继续？"):
            Thread(target=self._clear_all_history, daemon=True).start()
            self.update_status("正在清理所有RSS源的历史记录...")
    
    def _clear_all_history(self):
        """后台线程：清理所有RSS源的历史"""
        try:
            entries = HistoryManager._read_all_entries()
            
            # 按RSS源分组
            rss_groups = {}
            for entry in entries:
                rss_name = entry.get('rss_name')
                if rss_name:
                    if rss_name not in rss_groups:
                        rss_groups[rss_name] = []
                    rss_groups[rss_name].append(entry)
            
            # 每个源只保留最新的记录
            kept_entries = []
            for rss_name, group_entries in rss_groups.items():
                group_entries.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
                kept_entries.extend(group_entries[:MAX_DISPLAY_ENTRIES])
            
            # 写回文件
            HistoryManager._write_all_entries(kept_entries)
            
            self.root.after(0, lambda: self.update_status(f"✓ 历史已清理，共保留 {len(kept_entries)} 条记录"))
        except Exception as e:
            error_msg = f"清理失败: {str(e)}"
            self.root.after(0, lambda: self.update_status(error_msg, error=True))
    
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
            success_msg = f"✓ 成功下载 {len(urls)} 条"
            self.root.after(0, lambda: self.update_status(success_msg))
            self.root.after(0, self._render_paginated)
        except Exception as e:
            error_msg = f"下载失败: {str(e)}"
            self.root.after(0, lambda: self.update_status(error_msg, error=True))


if __name__ == "__main__":
    root = tk.Tk()
    app = RSSDownloaderApp(root)
    root.mainloop()

