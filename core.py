# 核心工具模块
"""
核心功能模块 - 统一工具集合

目录导航:
    - Line 30:  配置加载 (load_config)
    - Line 50:  正则表达式工具 (Regex)
    - Line 115: 文件操作工具 (read_json, write_json, read_lines, etc.)
    - Line 205: 日志管理器 (LoggerManager)
    - Line 255: 网络客户端 (WebClient)
    - Line 355: 数据库管理器 (DatabaseManager)
"""
import os
import re
import json
import logging
import threading
import sqlite3
import requests
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup


# ==================== 配置加载 ====================

# 配置文件路径
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'default.json')

# 加载配置
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = json.load(f)

# 导出运行时参数为全局常量
REQUEST_TIMEOUT = config['runtime']['REQUEST_TIMEOUT']
MAX_RETRIES = config['runtime']['MAX_RETRIES']
RETRY_DELAY = config['runtime']['RETRY_DELAY']
MAX_WORKERS = config['runtime']['MAX_WORKERS']
DOWN_SKIP_THRESHOLD = config['runtime']['DOWN_SKIP_THRESHOLD']
LOG_MAX_SIZE = config['runtime']['LOG_MAX_SIZE']
LOG_BACKUP_COUNT = config['runtime']['LOG_BACKUP_COUNT']
SAMPLE_ENABLED = config['runtime']['SAMPLE_ENABLED']
SAMPLE_COUNT = config['runtime']['SAMPLE_COUNT']
THUMBNAIL_SIZE = tuple(config['runtime']['THUMBNAIL_SIZE'])
CHUNK_SIZE = config['runtime']['CHUNK_SIZE']
RANDOM_DELAY_MIN = config['runtime']['RANDOM_DELAY_MIN']
RANDOM_DELAY_MAX = config['runtime']['RANDOM_DELAY_MAX']
FILE_COUNT_CHECK_INTERVAL = config['runtime']['FILE_COUNT_CHECK_INTERVAL']


# ==================== 正则表达式预编译 ====================

class Regex:
    """预编译正则表达式集合"""
    IMAGE_ID = re.compile(r'&id=(\d+)')
    POSTED_TIME = re.compile(r'Posted:.*')
    FILE_NUMBER = re.compile(r'(\d{1,4})_')
    ID_TEXT = re.compile(r'Id:.*')
    
    @classmethod
    def extract_image_id(cls, url: str) -> Optional[str]:
        """从URL提取图片ID"""
        match = cls.IMAGE_ID.search(url)
        return match.group(1) if match else None
    
    @classmethod
    def extract_posted_time(cls, text: str) -> Optional[str]:
        """提取发布时间 'Posted: 2023-12-01 15:30:45' -> '2023-12-01 15:30:45'"""
        match = cls.POSTED_TIME.search(text)
        return match.group(0)[8:] if match else None
    
    @classmethod
    def extract_id_text(cls, text: str) -> Optional[str]:
        """提取ID文本 'Id: 12345' -> '12345'"""
        match = cls.ID_TEXT.search(text)
        return match.group(0)[4:] if match else None
    
    @classmethod
    def extract_file_number(cls, filename: str) -> Optional[int]:
        """从文件名提取序号"""
        match = cls.FILE_NUMBER.match(filename)
        return int(match.group(1)) if match else None
    
    @staticmethod
    def extract_pic_url(soup) -> Optional[str]:
        """从soup提取图片URL"""
        try:
            meta = soup.find('meta', {'property': 'og:image'})
            return meta['content'] if meta else None
        except Exception:
            return None
    
    @staticmethod
    def extract_pic_tags(soup) -> Optional[str]:
        """从soup提取图片标签"""
        try:
            if soup.title and soup.title.string:
                return soup.title.string.split('- Image View -')[0].strip()
        except Exception:
            pass
        return None
    
    @staticmethod
    def extract_pic_filename(soup) -> Optional[str]:
        """从soup提取文件名"""
        try:
            meta = soup.find('meta', {'property': 'og:image'})
            if meta and meta.get('content'):
                return meta['content'].split('/')[-1]
        except Exception:
            pass
        return None


# ==================== 文件工具 ====================

def read_json(filepath: str) -> Dict[str, Any]:
    """读取JSON文件"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json(filepath: str, data: Dict[str, Any], indent: int = 2) -> bool:
    """写入JSON文件"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        return True
    except Exception:
        return False


def read_lines(filepath: str, strip: bool = True) -> List[str]:
    """按行读取文本"""
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        if strip:
            return [line.strip() for line in f if line.strip()]
        return [line.rstrip('\n') for line in f]


def write_lines(filepath: str, lines: List[str], mode: str = 'w') -> bool:
    """写入多行文本"""
    try:
        with open(filepath, mode, encoding='utf-8') as f:
            for line in lines:
                f.write(line + '\n')
        return True
    except Exception:
        return False


def append_line(filepath: str, line: str) -> bool:
    """追加单行"""
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
        return True
    except Exception:
        return False


def ensure_dir(dirpath: str) -> bool:
    """确保目录存在"""
    try:
        os.makedirs(dirpath, exist_ok=True)
        return True
    except Exception:
        return False


def safe_filename(filename: str, replacement: str = '_') -> str:
    """清理文件名特殊字符"""
    for char in ['<', '>', ':', '"', '/', '\\', '|', '?', '*', '.', '+']:
        filename = filename.replace(char, replacement)
    return filename


def format_size(size_bytes: int) -> str:
    """格式化文件大小（人类可读格式）"""
    if not size_bytes:
        return "0 B"
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / 1024 / 1024:.2f} MB"
    else:
        return f"{size_bytes / 1024 / 1024 / 1024:.2f} GB"


def load_tag_mapping(reverse: bool = False) -> dict:
    """
    加载tag映射关系
    
    Args:
        reverse: False返回 {original_tag: replace_tag}
                True返回 {replace_tag: original_tag}
    
    Returns:
        映射字典
    """
    mapping = {}
    try:
        tag_replace_path = config['path']['tag_replace']
        with open(tag_replace_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[1:]:  # 跳过header
                line = line.strip()
                if ',' in line:
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        original_tag, replace_tag = parts[0], parts[1]
                        if reverse:
                            mapping[replace_tag] = original_tag
                        else:
                            mapping[original_tag] = replace_tag
    except FileNotFoundError:
        pass  # 文件不存在，返回空字典
    except Exception as e:
        print(f"⚠️  读取tag映射文件失败: {e}")
    
    return mapping


def get_max_file_number(directory: str, pattern: str = r'(\d{1,4})_') -> int:
    """获取目录中最大文件序号"""
    max_num = 0
    regex = re.compile(pattern)
    
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_file():
                    match = regex.match(entry.name)
                    if match:
                        max_num = max(max_num, int(match.group(1)))
    except FileNotFoundError:
        pass  # 目录不存在
    except Exception:
        pass
    
    return max_num


# ==================== 线程安全日志 ====================

class LoggerManager:
    """线程安全日志管理器（单例）"""
    _instances = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_logger(cls, name: str, log_dir: Optional[str] = None) -> logging.Logger:
        """获取日志器（单例模式）"""
        with cls._lock:
            if name not in cls._instances:
                logger = logging.getLogger(f'gelbooru.{name}')
                logger.setLevel(logging.INFO)
                
                if not logger.handlers:
                    # 文件日志（支持轮转）
                    if log_dir:
                        ensure_dir(log_dir)
                        log_path = os.path.join(log_dir, f'z {name}.txt')
                        file_handler = RotatingFileHandler(
                            log_path,
                            maxBytes=LOG_MAX_SIZE,
                            backupCount=LOG_BACKUP_COUNT,
                            encoding='utf-8'
                        )
                        formatter = logging.Formatter(
                            '%(asctime)s | %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S'
                        )
                        file_handler.setFormatter(formatter)
                        logger.addHandler(file_handler)
                    
                    # 控制台日志
                    console = logging.StreamHandler()
                    console.setFormatter(logging.Formatter(
                        '%(asctime)s | %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                    ))
                    logger.addHandler(console)
                
                cls._instances[name] = logger
            
            return cls._instances[name]


def get_logger(name: str, log_dir: Optional[str] = None) -> logging.Logger:
    """获取日志器便捷函数"""
    return LoggerManager.get_logger(name, log_dir)


# ==================== 网络客户端 ====================

class WebClient:
    """网络客户端 - 整合下载和解析功能"""
    
    def __init__(self, headers: dict, timeout: int = 10, max_retries: int = 50):
        self.headers = headers
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(headers)
    
    def download_image(self, url: str, retries: Optional[int] = None) -> Optional[bytes]:
        """下载图片到内存"""
        max_attempts = retries if retries is not None else self.max_retries
        
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.content
            except (requests.RequestException, requests.Timeout):
                if attempt < max_attempts - 1:
                    time.sleep(2 ** min(attempt, 5))
                continue
            except Exception:
                return None
        return None
    
    def get_soup(self, url: str, retries: Optional[int] = None) -> Optional[BeautifulSoup]:
        """获取页面并解析为BeautifulSoup对象"""
        max_attempts = retries if retries is not None else self.max_retries
        
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return BeautifulSoup(response.text, 'html.parser')
            except (requests.RequestException, requests.Timeout):
                if attempt < max_attempts - 1:
                    time.sleep(2 ** min(attempt, 5))
                continue
            except Exception:
                return None
        return None
    
    @staticmethod
    def get_image_list(soup: BeautifulSoup) -> List[str]:
        """从列表页提取图片详情URL"""
        try:
            return [x.find_all('a')[0]['href'] for x in soup.find_all('article')]
        except Exception:
            return []
    
    @staticmethod
    def get_image_ids(soup: BeautifulSoup) -> List[str]:
        """从列表页提取图片ID"""
        try:
            articles = soup.find_all('article')
            return [re.findall(r'&id=(\d+)', x.find_all('a')[0]['href'])[0] for x in articles]
        except Exception:
            return []
    
    def close(self):
        """关闭会话"""
        self.session.close()


# ==================== 数据库管理 ====================

class DatabaseManager:
    """线程安全的数据库管理器（单例模式）"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path: Optional[str] = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path: Optional[str] = None):
        if self._initialized:
            return
        
        if db_path is None:
            db_path = r'F:\Pic\Gelbooru\new\gelbooru_metadata.db'
        
        self.db_path = db_path
        self._thread_local = threading.local()
        self._initialized = True
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取线程本地连接"""
        if not hasattr(self._thread_local, 'connection'):
            self._thread_local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._thread_local.connection.row_factory = sqlite3.Row
            # 启用 WAL 模式提升并发性能
            self._thread_local.connection.execute('PRAGMA journal_mode=WAL')
            self._thread_local.connection.execute('PRAGMA synchronous=NORMAL')
        return self._thread_local.connection
    
    @contextmanager
    def get_cursor(self):
        """获取游标的上下文管理器"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def _init_database(self):
        """初始化数据库结构"""
        # 确保数据库目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        with self.get_cursor() as cursor:
            # 1. 图片信息表（保留所有原始元数据）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pictures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pic_id TEXT NOT NULL,
                    tag_name TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    new_filename TEXT,
                    file_path TEXT NOT NULL,
                    file_size INTEGER,
                    pic_url TEXT,
                    pic_tags TEXT,
                    pic_time TEXT,
                    pic_date TEXT,
                    download_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'downloaded',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(pic_id, tag_name)
                )
            """)
            
            # 2. 失败记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS failed_downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag_name TEXT NOT NULL,
                    pic_url TEXT NOT NULL,
                    pic_time TEXT,
                    pic_id TEXT,
                    pic_filename TEXT,
                    pic_tags TEXT,
           created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 3. 标签下载进度表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tag_progress (
                    tag TEXT PRIMARY KEY,
                    startpage INTEGER DEFAULT 1,
                    endpage INTEGER DEFAULT 1,
                    start_pic INTEGER DEFAULT 0,
                    end_pic TEXT DEFAULT '0',
                    status INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pictures_tag_name ON pictures(tag_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pictures_pic_id ON pictures(pic_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pictures_filename ON pictures(filename)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_progress_status ON tag_progress(status)')
    
    # ============ 图片操作 ============
    
    def add_picture(self, pic_data: Dict[str, Any]) -> int:
        """添加图片记录"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO pictures 
                (pic_id, tag_name, filename, new_filename, file_path, 
                 file_size, pic_url, pic_tags, pic_time, pic_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pic_data['pic_id'],
                pic_data['tag_name'],
                pic_data['filename'],
                pic_data.get('new_filename'),
                pic_data['file_path'],
                pic_data.get('file_size'),
                pic_data.get('pic_url'),
                pic_data.get('pic_tags'),
                pic_data.get('pic_time'),
                pic_data.get('pic_date')
            ))
            return cursor.lastrowid
    
    def picture_exists(self, pic_id: str, tag_name: Optional[str] = None) -> bool:
        """检查图片是否存在"""
        with self.get_cursor() as cursor:
            if tag_name:
                cursor.execute(
                    'SELECT 1 FROM pictures WHERE pic_id=? AND tag_name=? LIMIT 1',
                    (pic_id, tag_name)
                )
            else:
                cursor.execute(
                    'SELECT 1 FROM pictures WHERE pic_id=? LIMIT 1',
            (pic_id,)
                )
            return cursor.fetchone() is not None
    
    def get_pictures_by_tag(self, tag_name: str, limit: Optional[int] = None) -> List[Dict]:
        """获取标签下的所有图片"""
        with self.get_cursor() as cursor:
            query = 'SELECT * FROM pictures WHERE tag_name=? ORDER BY pic_time DESC'
            if limit:
                query += f' LIMIT {limit}'
            cursor.execute(query, (tag_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_picture_by_filename(self, filename: str) -> Optional[Dict]:
        """根据文件名查询图片信息"""
        with self.get_cursor() as cursor:
            cursor.execute('SELECT * FROM pictures WHERE filename=? LIMIT 1', (filename,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def search_pictures_by_tags(self, tags: List[str], match_all: bool = False) -> List[Dict]:
        """
        根据标签搜索图片
        
        Args:
            tags: 标签列表，例如 ['tag1', 'tag_2']
            match_all: True=必须包含所有标签，False=包含任意标签
        
        Returns:
            图片列表
        """
        with self.get_cursor() as cursor:
            if match_all:
                # 必须包含所有标签
                conditions = ' AND '.join(['pic_tags LIKE ?' for _ in tags])
                params = [f'%{tag}%' for tag in tags]
                query = f'SELECT * FROM pictures WHERE {conditions} ORDER BY pic_time DESC'
            else:
                # 包含任意标签
                conditions = ' OR '.join(['pic_tags LIKE ?' for _ in tags])
                params = [f'%{tag}%' for tag in tags]
                query = f'SELECT * FROM pictures WHERE {conditions} ORDER BY pic_time DESC'
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    # ============ 失败记录操作 ============
    
    def add_failed_download(self, tag_name: str, pic_url: str, pic_time: str, 
                           pic_id: str, pic_filename: str, pic_tags: str):
        """记录失败的下载"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO failed_downloads 
                (tag_name, pic_url, pic_time, pic_id, pic_filename, pic_tags)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (tag_name, pic_url, pic_time, pic_id, pic_filename, pic_tags))
    
    # ============ 标签进度操作 ============
    
    def init_tag_progress(self, tag: str, endpage: int, start_pic: int = 0, 
      end_pic: str = '0', status: int = 0):
        """初始化标签下载进度"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO tag_progress 
                (tag, startpage, endpage, start_pic, end_pic, status, updated_at)
                VALUES (?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (tag, endpage, start_pic, end_pic, status))
    
    def get_tag_progress(self, tag: str) -> Optional[Dict]:
        """获取标签下载进度"""
        with self.get_cursor() as cursor:
            cursor.execute('SELECT * FROM tag_progress WHERE tag=?', (tag,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_all_tag_progress(self, status_filter: Optional[List[int]] = None) -> Dict[str, Dict]:
        """获取所有标签进度"""
        with self.get_cursor() as cursor:
            if status_filter:
                placeholders = ','.join('?' * len(status_filter))
                cursor.execute(
                    f'SELECT * FROM tag_progress WHERE status IN ({placeholders})',
                    status_filter
                )
            else:
                cursor.execute('SELECT * FROM tag_progress')
            
            result = {}
            for row in cursor.fetchall():
                tag = row['tag']
                result[tag] = {
                    'startpage': row['startpage'],
                    'endpage': row['endpage'],
                    'start_pic': row['start_pic'],
                    'end_pic': row['end_pic'],
                    'status': row['status']
                }
            return result
    
    def update_tag_progress(self, tag: str, startpage: Optional[int] = None,
                           start_pic: Optional[int] = None, status: Optional[int] = None):
        """更新标签进度"""
        updates = []
        params = []
        
        if startpage is not None:
            updates.append('startpage=?')
            params.append(startpage)
        if start_pic is not None:
            updates.append('start_pic=?')
            params.append(start_pic)
        if status is not None:
            updates.append('status=?')
            params.append(status)
        
        if not updates:
            return
        
        updates.append('updated_at=CURRENT_TIMESTAMP')
        params.append(tag)
        
        with self.get_cursor() as cursor:
            cursor.execute(
                f'UPDATE tag_progress SET {", ".join(updates)} WHERE tag=?',
                params
            )
    
    def delete_tag_progress(self, tag: str):
        """删除标签进度"""
        with self.get_cursor() as cursor:
            cursor.execute('DELETE FROM tag_progress WHERE tag=?', (tag,))
    
    def close_all_connections(self):
        """关闭所有连接"""
        if hasattr(self._thread_local, 'connection'):
            self._thread_local.connection.close()
            delattr(self._thread_local, 'connection')


def get_database(db_path: Optional[str] = None) -> DatabaseManager:
    """获取数据库实例的便捷函数"""
    return DatabaseManager(db_path)

