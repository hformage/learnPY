"""
数据库查询工具 - Gelbooru图片元数据查询

使用方法:
    python check_db_log.py
    
功能:
    1. 根据tag查询该tag下所有图片
    2. 根据图片文件名查询图片信息
    3. 根据图片标签(pic_tags)搜索图片
    4. 插入/更新/删除图片记录

说明:
    这是一个独立的数据库查询工具，不依赖项目的其他模块
    只需要访问数据库文件即可运行
"""

import sys
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import List, Dict, Optional


# ==================== 独立的数据库管理器 ====================

class DatabaseManager:
    """线程安全的数据库管理器（查询工具专用）"""
    
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
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取线程本地连接"""
        if not hasattr(self._thread_local, 'connection'):
            self._thread_local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._thread_local.connection.row_factory = sqlite3.Row
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
            tags: 标签列表
            match_all: True=必须包含所有标签，False=包含任意标签
        """
        with self.get_cursor() as cursor:
            if match_all:
                conditions = ' AND '.join(['pic_tags LIKE ?' for _ in tags])
            else:
                conditions = ' OR '.join(['pic_tags LIKE ?' for _ in tags])
            
            params = [f'%{tag}%' for tag in tags]
            query = f'SELECT * FROM pictures WHERE {conditions} ORDER BY pic_time DESC'
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def add_picture(self, pic_data: Dict) -> int:
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


# ==================== 查询工具类 ====================

class DBQueryTool:
    """数据库查询工具类"""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        初始化查询工具
        
        Args:
            db_path: 数据库文件路径，默认为 F:\Pic\Gelbooru\new\gelbooru_metadata.db
        """
        self.db = DatabaseManager(db_path)
    
    def query_by_tag(self, tag_name: str, limit: Optional[int] = None):
        """
        1. 根据tag查询所有图片
        
        Args:
            tag_name: 标签名（如 'character_name'）
            limit: 限制返回数量，None表示全部
 """
        print(f"\n{'='*80}")
        print(f"📂 查询标签: {tag_name}")
        print(f"{'='*80}\n")
        
        pictures = self.db.get_pictures_by_tag(tag_name, limit)
        
        if not pictures:
            print(f"❌ 未找到标签 '{tag_name}' 下的图片")
            return []
        
        print(f"✅ 共找到 {len(pictures)} 张图片\n")
        
        for idx, pic in enumerate(pictures, 1):
            print(f"[{idx}] 图片信息:")
            print(f"  📌 文件名: {pic['filename']}")
            print(f"  📁 路径: {pic['file_path']}")
            print(f"  🆔 图片ID: {pic['pic_id']}")
            print(f"  📅 上传日期: {pic['pic_date']}")
            print(f"  🕐 上传时间: {pic['pic_time']}")
            print(f"  📊 文件大小: {self._format_size(pic['file_size'])}")
            print(f"  🏷️  标签列表: {pic['pic_tags'][:100]}..." if len(pic.get('pic_tags', '')) > 100 else f"  🏷️  标签列表: {pic['pic_tags']}")
            print()
        
        return pictures
    
    def query_by_filename(self, filename: str):
        """
        2. 根据文件名查询图片信息
        
        Args:
            filename: 文件名（如 'xxxxxx.jpg'）
        """
        print(f"\n{'='*80}")
        print(f"🔍 查询文件: {filename}")
        print(f"{'='*80}\n")
        
        pic = self.db.get_picture_by_filename(filename)
        
        if not pic:
            print(f"❌ 未找到文件 '{filename}'")
            return None
        
        print("✅ 找到图片信息:\n")
        print(f"  📂 所属标签: {pic['tag_name']}")
        print(f"  📁 存储路径: {pic['file_path']}")
        print(f"  🆔 图片ID: {pic['pic_id']}")
        print(f"  📅 上传日期: {pic['pic_date']}")
        print(f"  🕐 上传时间: {pic['pic_time']}")
        print(f"  📊 文件大小: {self._format_size(pic['file_size'])}")
        print(f"  🔗 原始URL: {pic['pic_url']}")
        print(f"  🏷️  完整标签: {pic['pic_tags']}")
        print()
        
        return pic
    
    def query_by_pic_tags(self, tags: List[str], match_all: bool = False):
        """
        3. 根据图片标签(pic_tags)搜索图片
        
        Args:
            tags: 标签列表，例如 ['tt aa', 't4'] 或 ['tag1', 'tag_2']
            match_all: True=必须包含所有标签，False=包含任意标签
        
        示例:
            旧代码中 pic_tags = "tag1, tag_2, tt aa, t4, tt5"
            输入 ['tt aa', 't4'] 可以匹配到这张图片
        """
        print(f"\n{'='*80}")
        print(f"🔎 搜索标签: {', '.join(tags)}")
        print(f"   匹配模式: {'全部匹配' if match_all else '任意匹配'}")
        print(f"{'='*80}\n")
        
        pictures = self.db.search_pictures_by_tags(tags, match_all)
        
        if not pictures:
            print(f"❌ 未找到包含标签 {tags} 的图片")
            return []
        
        print(f"✅ 共找到 {len(pictures)} 张图片\n")
        
        for idx, pic in enumerate(pictures, 1):
            print(f"[{idx}] 图片信息:")
            print(f"  📂 所属标签: {pic['tag_name']}")
            print(f"  📌 文件名: {pic['filename']}")
            print(f"  📁 路径: {pic['file_path']}")
            print(f"  🆔 图片ID: {pic['pic_id']}")
            print(f"  📅 日期: {pic['pic_date']}")
            print(f"  🏷️  匹配标签: {pic['pic_tags'][:120]}..." if len(pic.get('pic_tags', '')) > 120 else f"  🏷️  标签: {pic['pic_tags']}")
            print()
        
        return pictures
    
    def insert_picture(self, pic_data: Dict):
        """
        插入图片记录
        
        Args:
            pic_data: 图片数据字典，必须包含:
                - pic_id: 图片ID
                - tag_name: 标签名
                - filename: 文件名
                - file_path: 文件路径
                可选:
                - file_size, pic_url, pic_tags, pic_time, pic_date, new_filename
        """
        try:
            pic_id = self.db.add_picture(pic_data)
            print(f"✅ 成功插入图片记录 (ID: {pic_id})")
            return pic_id
        except Exception as e:
            print(f"❌ 插入失败: {e}")
            return None
    
    def update_picture(self, pic_id: str, tag_name: str, updates: Dict):
        """
        更新图片记录（通过删除后插入实现）
        
        Args:
            pic_id: 图片ID
            tag_name: 标签名
            updates: 要更新的字段字典
        """
        print(f"⚠️  当前数据库使用 INSERT OR REPLACE 策略")
        print(f"   建议使用 insert_picture() 方法，会自动覆盖已存在的记录")
    
    def delete_picture(self, pic_id: str, tag_name: str):
        """
        删除图片记录
        
        Args:
            pic_id: 图片ID
            tag_name: 标签名
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(
                'DELETE FROM pictures WHERE pic_id=? AND tag_name=?',
                (pic_id, tag_name)
            )
            if cursor.rowcount > 0:
                print(f"✅ 成功删除图片记录 (ID: {pic_id}, Tag: {tag_name})")
            else:
                print(f"❌ 未找到该记录")
    
    def get_statistics(self):
        """获取数据库统计信息"""
        print(f"\n{'='*80}")
        print(f"📊 数据库统计")
        print(f"{'='*80}\n")
        
        with self.db.get_cursor() as cursor:
            # 总图片数
            cursor.execute('SELECT COUNT(*) as cnt FROM pictures')
            total_pics = cursor.fetchone()['cnt']
            
            # 总标签数
            cursor.execute('SELECT COUNT(DISTINCT tag_name) as cnt FROM pictures')
            total_tags = cursor.fetchone()['cnt']
            
            # 总文件大小
            cursor.execute('SELECT SUM(file_size) as total FROM pictures')
            total_size = cursor.fetchone()['total'] or 0
            
            # 失败记录数
            cursor.execute('SELECT COUNT(*) as cnt FROM failed_downloads')
            failed_cnt = cursor.fetchone()['cnt']
            
            print(f"  📷 总图片数: {total_pics}")
            print(f"  🏷️  总标签数: {total_tags}")
            print(f"  📊 总大小: {self._format_size(total_size)}")
            print(f"  ❌ 失败记录: {failed_cnt}")
            print()
    
    def list_all_tags(self):
        """列出所有标签"""
        with self.db.get_cursor() as cursor:
            cursor.execute("""
                SELECT tag_name, COUNT(*) as pic_count, SUM(file_size) as total_size
                FROM pictures 
                GROUP BY tag_name 
                ORDER BY pic_count DESC
            """)
            tags = cursor.fetchall()
        
        print(f"\n{'='*80}")
        print(f"📋 所有标签列表 (共 {len(tags)} 个)")
        print(f"{'='*80}\n")
        
        for idx, tag in enumerate(tags, 1):
            print(f"[{idx:3}] {tag['tag_name']:<30} | 图片: {tag['pic_count']:>5} 张 | 大小: {self._format_size(tag['total_size'])}")
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小"""
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


def print_menu():
    """打印菜单"""
    print("\n" + "="*80)
    print("  Gelbooru 数据库查询工具 v1.0")
    print("="*80)
    print("\n  查询功能:")
    print("    1. 根据tag查询图片")
    print("    2. 根据文件名查询")
    print("    3. 根据图片标签搜索")
    print("    4. 列出所有标签")
    print("    5. 数据库统计")
    print("\n  管理功能:")
    print("    6. 插入图片记录")
    print("    7. 删除图片记录")
    print("\n    0. 退出")
    print("="*80)


def main():
    """主函数"""
    tool = DBQueryTool()
    
    while True:
        print_menu()
        choice = input("\n请选择功能 (0-7): ").strip()
        
        if choice == '0':
            print("\n👋 再见！")
            break
        
        elif choice == '1':
            tag = input("请输入标签名: ").strip()
            limit_str = input("限制数量（回车=全部）: ").strip()
            limit = int(limit_str) if limit_str else None
            tool.query_by_tag(tag, limit)
        
        elif choice == '2':
            filename = input("请输入文件名: ").strip()
            tool.query_by_filename(filename)
        
        elif choice == '3':
            tags_input = input("请输入标签（逗号分隔，如: tt aa, t4）: ").strip()
            tags = [t.strip() for t in tags_input.split(',')]
            match_all_input = input("匹配模式 (1=全部匹配, 0=任意匹配, 默认0): ").strip()
            match_all = match_all_input == '1'
            tool.query_by_pic_tags(tags, match_all)
        
        elif choice == '4':
            tool.list_all_tags()
        
        elif choice == '5':
            tool.get_statistics()
        
        elif choice == '6':
            print("\n请输入图片信息（必填项）:")
            pic_data = {
                'pic_id': input("  图片ID: ").strip(),
                'tag_name': input("  标签名: ").strip(),
                'filename': input("  文件名: ").strip(),
                'file_path': input("  文件路径: ").strip(),
            }
            print("\n可选项（回车跳过）:")
            file_size = input("  文件大小(字节): ").strip()
            if file_size:
                pic_data['file_size'] = int(file_size)
            pic_url = input("  图片URL: ").strip()
            if pic_url:
                pic_data['pic_url'] = pic_url
            pic_tags = input("  图片标签: ").strip()
            if pic_tags:
                pic_data['pic_tags'] = pic_tags
            pic_time = input("  上传时间: ").strip()
            if pic_time:
                pic_data['pic_time'] = pic_time
            pic_date = input("  上传日期: ").strip()
            if pic_date:
                pic_data['pic_date'] = pic_date
            
            tool.insert_picture(pic_data)
        
        elif choice == '7':
            pic_id = input("请输入图片ID: ").strip()
            tag_name = input("请输入标签名: ").strip()
            confirm = input(f"确认删除 '{tag_name}' 下的图片 '{pic_id}' ? (y/n): ").strip().lower()
            if confirm == 'y':
                tool.delete_picture(pic_id, tag_name)
        
        else:
            print("\n❌ 无效选择，请重新输入")
        
        input("\n按回车继续...")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 程序已终止")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()

