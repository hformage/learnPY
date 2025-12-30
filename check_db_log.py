"""
数据库查询工具 - Gelbooru图片元数据查询 (GUI版本)

使用方法:
    python check_db_log.py
    
功能:
    1. 根据tag查询该tag下所有图片
    2. 根据图片ID查询tag名字
    3. 根据tags搜索图片
    4. 自定义SQL查询
    5. 显示所有表名

说明:
    这是一个独立的数据库查询工具，不依赖项目的其他模块
    只需要访问数据库文件即可运行
"""

import os
import sqlite3
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from contextlib import contextmanager
from typing import List, Dict, Optional


# ==================== 独立的数据库管理器 ====================

class DatabaseManager:
    """数据库管理器（非单例，支持动态切换路径）"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._connection.row_factory = sqlite3.Row
        return self._connection
    
    def close(self):
        """关闭连接"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
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
    
    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cursor.fetchall()]
    
    def get_pictures_by_tag(self, tag_name: str) -> List[Dict]:
        """获取标签下的所有图片"""
        with self.get_cursor() as cursor:
            cursor.execute('SELECT * FROM pictures WHERE tag_name=? ORDER BY pic_time DESC', (tag_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_tag_by_pic_id(self, pic_id: str) -> List[Dict]:
        """根据图片ID查询tag信息"""
        with self.get_cursor() as cursor:
            cursor.execute('SELECT * FROM pictures WHERE pic_id=?', (pic_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_tag_by_filename(self, filename: str) -> List[Dict]:
        """根据文件名查询tag信息（支持带或不带扩展名）"""
        with self.get_cursor() as cursor:
            # 如果输入不包含扩展名，使用LIKE模糊匹配
            if '.' not in filename:
                cursor.execute('SELECT * FROM pictures WHERE filename LIKE ?', (f'{filename}.%',))
            else:
                cursor.execute('SELECT * FROM pictures WHERE filename=?', (filename,))
            return [dict(row) for row in cursor.fetchall()]
    
    def search_pictures_by_tags(self, tags: List[str]) -> List[Dict]:
        """根据标签搜索图片（任意匹配）"""
        with self.get_cursor() as cursor:
            conditions = ' OR '.join(['pic_tags LIKE ?' for _ in tags])
            params = [f'%{tag}%' for tag in tags]
            query = f'SELECT * FROM pictures WHERE {conditions} ORDER BY pic_time DESC'
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_tags(self) -> List[str]:
        """获取所有唯一的tag_name"""
        with self.get_cursor() as cursor:
            cursor.execute('SELECT DISTINCT tag_name FROM pictures ORDER BY tag_name')
            return [row[0] for row in cursor.fetchall()]
    
    def execute_sql(self, sql: str) -> tuple:
        """执行自定义SQL，返回(columns, rows)"""
        with self.get_cursor() as cursor:
            cursor.execute(sql)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = [dict(row) for row in cursor.fetchall()]
                return columns, rows
            return [], []


# ==================== GUI 应用 ====================

class DBQueryGUI:
    """数据库查询GUI"""
    
    DEFAULT_DB_PATH = r'F:\Pic\Gelbooru\new\gelbooru_metadata.db'
    
    def __init__(self, root):
        self.root = root
        self.root.title("Gelbooru 数据库查询工具")
        self.root.geometry("900x700")
        self.root.minsize(700, 500)
        
        # 配置参数
        self.separator_length = 30  # 分隔符长度
        self.default_rows_per_page = 10  # 默认每页行数
        self.max_value_length = 200  # 结果字段最大显示长度
        
        self.db = None
        self.current_results = []
        self.current_index = 0
        self.row_number = self.default_rows_per_page
        
        self._create_widgets()
        self._bind_events()
        
        # 初始化数据库连接
        self.db_path_var.set(self.DEFAULT_DB_PATH)
        self._connect_db()
    
    def _create_widgets(self):
        """创建所有控件"""
  # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ===== 数据库路径 =====
        path_frame = ttk.Frame(main_frame)
        path_frame.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(path_frame, text="db_path:").pack(side=tk.LEFT)
        self.db_path_var = tk.StringVar()
        self.db_path_entry = ttk.Entry(path_frame, textvariable=self.db_path_var, width=80)
        self.db_path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(path_frame, text="连接", command=self._connect_db, width=8).pack(side=tk.LEFT)
        
        # 分割线
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)
        
        # ===== 显示所有表名 / 显示所有tag =====
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text="显示所有表名", command=self._show_tables).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="显示所有tag", command=self._show_all_tags).pack(side=tk.LEFT, padx=10)
        
        # 分割线
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)
        
        # ===== 查询区域 =====
        query_frame = ttk.Frame(main_frame)
        query_frame.pack(fill=tk.X, pady=5)
        
        # 查询tag下所有图片
        row1 = ttk.Frame(query_frame)
        row1.pack(fill=tk.X, pady=2)
        ttk.Label(row1, text="查询tag下所有图片:", width=20, anchor='w').pack(side=tk.LEFT)
        self.tag_entry = ttk.Entry(row1, width=50)
        self.tag_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(row1, text="Search", command=self._search_by_tag, width=10).pack(side=tk.LEFT)
        ttk.Button(row1, text="Clear", command=self._clear_input_fields, width=8).pack(side=tk.RIGHT)
        
        # 根据图片ID查询tag名字
        row2 = ttk.Frame(query_frame)
        row2.pack(fill=tk.X, pady=2)
        ttk.Label(row2, text="查询tag名字(图片ID):", width=20, anchor='w').pack(side=tk.LEFT)
        self.pic_id_entry = ttk.Entry(row2, width=50)
        self.pic_id_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="Search", command=self._search_by_pic_id, width=10).pack(side=tk.LEFT)
        
        # 根据文件名查询tag名字
        row2b = ttk.Frame(query_frame)
        row2b.pack(fill=tk.X, pady=2)
        ttk.Label(row2b, text="查询tag名字(文件名):", width=20, anchor='w').pack(side=tk.LEFT)
        self.filename_entry = ttk.Entry(row2b, width=50)
        self.filename_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(row2b, text="Search", command=self._search_by_filename, width=10).pack(side=tk.LEFT)
        ttk.Label(row2b, text="(xxxx 或 xxxx.jpg)", foreground="gray").pack(side=tk.LEFT, padx=5)
        
        # 根据tags查找图片
        row3 = ttk.Frame(query_frame)
        row3.pack(fill=tk.X, pady=2)
        ttk.Label(row3, text="根据tag查找图片:", width=20, anchor='w').pack(side=tk.LEFT)
        self.tags_entry = ttk.Entry(row3, width=50)
        self.tags_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(row3, text="Search", command=self._search_by_tags, width=10).pack(side=tk.LEFT)
        ttk.Label(row3, text="(逗号分隔)", foreground="gray").pack(side=tk.LEFT, padx=5)
        
        # 分割线
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)
        
        # ===== 自定义SQL =====
        sql_header = ttk.Frame(main_frame)
        sql_header.pack(fill=tk.X)
        ttk.Label(sql_header, text="查询SQL:").pack(side=tk.LEFT)
        ttk.Button(sql_header, text="Search", command=self._execute_sql).pack(side=tk.RIGHT)
        
        self.sql_text = scrolledtext.ScrolledText(main_frame, height=5, wrap=tk.WORD)
        self.sql_text.pack(fill=tk.X, pady=5)
        
        # 分割线
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)
        
        # ===== 结果控制 =====
        result_control = ttk.Frame(main_frame)
        result_control.pack(fill=tk.X, pady=5)
        
        ttk.Label(result_control, text="结果:").pack(side=tk.LEFT)
        ttk.Label(result_control, text="每页行数:").pack(side=tk.LEFT, padx=(10, 0))
        self.row_number_var = tk.StringVar(value=str(self.default_rows_per_page))
        self.row_number_entry = ttk.Entry(result_control, textvariable=self.row_number_var, width=5)
        self.row_number_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(result_control, text="下一页", command=self._next_page).pack(side=tk.LEFT, padx=5)
        ttk.Button(result_control, text="所有结果", command=self._show_all).pack(side=tk.LEFT, padx=5)
        
        # Clear按钮（先pack，显示在最右边）
        ttk.Button(result_control, text="Clear", command=self._clear_display).pack(side=tk.RIGHT, padx=5)
        
        # 结果统计（后pack，显示在Clear按钮左边）
        self.result_label = ttk.Label(result_control, text="查询结果: 0/0")
        self.result_label.pack(side=tk.RIGHT)
        
        # ===== 结果显示 =====
        self.result_text = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD)
        self.result_text.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 注意：已移除水平滚动条，因为 wrap=tk.WORD 会自动换行
    
    def _bind_events(self):
        """绑定事件"""
        self.tag_entry.bind('<Return>', lambda e: self._search_by_tag())
        self.pic_id_entry.bind('<Return>', lambda e: self._search_by_pic_id())
        self.filename_entry.bind('<Return>', lambda e: self._search_by_filename())
        self.tags_entry.bind('<Return>', lambda e: self._search_by_tags())
        self.row_number_entry.bind('<Return>', lambda e: self._update_row_number())
    
    def _connect_db(self):
        """连接数据库"""
        db_path = self.db_path_var.get().strip()
        if not db_path:
            messagebox.showerror("错误", "请输入数据库路径")
            return
        
        if not os.path.exists(db_path):
            messagebox.showerror("错误", f"数据库文件不存在: {db_path}")
            return
        
        try:
            if self.db:
                self.db.close()
            self.db = DatabaseManager(db_path)
            # 测试连接
            self.db.get_all_tables()
            self._show_message(f"✓ 数据库连接成功: {db_path}")
        except Exception as e:
            messagebox.showerror("错误", f"连接失败: {e}")
    
    def _show_tables(self):
        """显示所有表名"""
        if not self._check_db():
            return
        
        try:
            tables = self.db.get_all_tables()
            self._clear_results()
            self._append_message(f"数据库中的表 ({len(tables)} 个):\n" + "\n".join(f"  - {t}" for t in tables))
        except Exception as e:
            self._append_message(f"❌ 查询失败: {e}")
    
    def _show_all_tags(self):
        """显示所有唯一的tag_name"""
        if not self._check_db():
            return
        
        try:
            tags = self.db.get_all_tags()
            self._clear_results()
            if tags:
                tags_str = ", ".join(tags)
                self._append_message(f"数据库中的所有tag ({len(tags)} 个):\n\n{tags_str}")
            else:
                self._append_message("数据库中没有任何tag记录")
        except Exception as e:
            self._append_message(f"❌ 查询失败: {e}")
    
    def _execute_query(self, query_func, input_value, title_prefix, error_msg="请输入查询内容"):
        """通用查询执行方法"""
        if not self._check_db():
            return
        
        if not input_value:
            messagebox.showwarning("提示", error_msg)
            return
        
        try:
            results = query_func(input_value)
            self._display_results(results, f"{title_prefix}: {input_value}")
        except Exception as e:
            self._append_message(f"❌ 查询失败: {e}")
    
    def _search_by_tag(self):
        """根据tag查询"""
        tag = self.tag_entry.get().strip()
        self._execute_query(self.db.get_pictures_by_tag, tag, "Tag", "请输入tag名称")
    
    def _clear_input_fields(self):
        """清空所有输入框"""
        self.tag_entry.delete(0, tk.END)
        self.pic_id_entry.delete(0, tk.END)
        self.filename_entry.delete(0, tk.END)
        self.tags_entry.delete(0, tk.END)
    
    def _search_by_pic_id(self):
        """根据图片ID查询"""
        pic_id = self.pic_id_entry.get().strip()
        self._execute_query(self.db.get_tag_by_pic_id, pic_id, "图片ID", "请输入图片ID")
    
    def _search_by_filename(self):
        """根据文件名查询"""
        filename = self.filename_entry.get().strip()
        self._execute_query(self.db.get_tag_by_filename, filename, "文件名", "请输入文件名")
    
    def _search_by_tags(self):
        """根据tags搜索"""
        tags_input = self.tags_entry.get().strip()
        if not tags_input:
            messagebox.showwarning("提示", "请输入标签")
            return
        
        # 处理输入：替换中文逗号，去除空格
        tags_input = tags_input.replace('，', ',').replace('、', ',')
        tags = [t.strip() for t in tags_input.split(',') if t.strip()]
        
        if not tags:
            messagebox.showwarning("提示", "请输入有效的标签")
            return
        
        if not self._check_db():
            return
        
        try:
            results = self.db.search_pictures_by_tags(tags)
            self._display_results(results, f"Tags: {', '.join(tags)}")
        except Exception as e:
            self._append_message(f"❌ 查询失败: {e}")
    
    def _execute_sql(self):
        """执行自定义SQL"""
        if not self._check_db():
            return
        
        sql = self.sql_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("提示", "请输入SQL语句")
            return
        
        try:
            columns, results = self.db.execute_sql(sql)
            if results:
                self._display_results(results, f"SQL查询")
            else:
                self._append_message("✓ SQL执行成功，无返回数据")
        except Exception as e:
            self._append_message(f"❌ SQL执行失败: {e}")
    
    def _check_db(self):
        """检查数据库连接"""
        if not self.db:
            messagebox.showwarning("提示", "请先连接数据库")
            return False
        return True
    
    def _clear_results(self):
        """清空分页状态（不清除显示）"""
        self.current_results = []
        self.current_index = 0
        self.result_label.config(text="查询结果: 0/0")
    
    def _clear_display(self):
        """清空结果显示框"""
        self.result_text.delete("1.0", tk.END)
        self.current_results = []
        self.current_index = 0
        self.result_label.config(text="查询结果: 0/0")
    
    def _show_message(self, msg):
        """显示消息（覆盖模式，用于连接成功等提示）"""
        self.result_text.delete("1.0", tk.END)
        self.result_text.insert(tk.END, msg)
    
    def _append_message(self, msg):
        """追加消息（不清除原有内容）"""
        if self.result_text.get("1.0", tk.END).strip():
            separator = "-" * self.separator_length
            self.result_text.insert(tk.END, f"\n\n{separator}\n\n")
        self.result_text.insert(tk.END, msg)
        self.result_text.see(tk.END)
    
    def _update_row_number(self):
        """更新每页行数"""
        try:
            self.row_number = int(self.row_number_var.get())
            if self.row_number < 1:
                self.row_number = self.default_rows_per_page
                self.row_number_var.set(str(self.default_rows_per_page))
        except ValueError:
            self.row_number = self.default_rows_per_page
            self.row_number_var.set(str(self.default_rows_per_page))
    
    def _format_row(self, row_data: Dict) -> str:
        """格式化单行数据为字符串"""
        lines = []
        for key, value in row_data.items():
            str_value = str(value) if value is not None else "NULL"
            if len(str_value) > self.max_value_length:
                str_value = str_value[:self.max_value_length] + "..."
            lines.append(f"  {key}: {str_value}")
        return "\n".join(lines)
    
    def _display_results(self, results, title=""):
        """显示查询结果（追加模式）"""
        self._update_row_number()
        self.current_results = results
        self.current_index = 0
        
        if not results:
            self._append_message(f"{title}\n\n❌ 未找到任何记录")
            self.result_label.config(text="查询结果: 0/0")
            return
        
        self._show_current_page(title)
    
    def _show_current_page(self, title=""):
        """显示当前页（追加模式）"""
        total = len(self.current_results)
        start = self.current_index
        end = min(start + self.row_number, total)
        
        # 如果已有内容，先添加分隔
        if self.result_text.get("1.0", tk.END).strip():
            separator = "-" * self.separator_length
            self.result_text.insert(tk.END, f"\n\n{separator}\n\n")
        
        # 显示标题和统计
        if title:
            self.result_text.insert(tk.END, f"=== {title} ===\n\n")
        
        # 显示当前页数据
        for i, row in enumerate(self.current_results[start:end], start + 1):
            self.result_text.insert(tk.END, f"[{i}] ----------------------------------------\n")
            self.result_text.insert(tk.END, self._format_row(row) + "\n\n")
        
        self.result_label.config(text=f"查询结果: {end}/{total}")
        self.result_text.see(tk.END)
    
    def _next_page(self):
        """下一页"""
        if not self.current_results:
            return
        
        self._update_row_number()
        total = len(self.current_results)
        
        if self.current_index + self.row_number < total:
            self.current_index += self.row_number
            self._show_current_page()
    
    def _show_all(self):
        """显示所有结果（追加模式）"""
        if not self.current_results:
            return
        
        # 如果已有内容，先添加分隔
        if self.result_text.get("1.0", tk.END).strip():
            separator = "-" * self.separator_length
            self.result_text.insert(tk.END, f"\n\n{separator}\n=== 显示全部结果 ===\n{separator}\n\n")
        
        total = len(self.current_results)
        
        for i, row in enumerate(self.current_results, 1):
            self.result_text.insert(tk.END, f"[{i}] ----------------------------------------\n")
            self.result_text.insert(tk.END, self._format_row(row) + "\n\n")
        
        self.result_label.config(text=f"查询结果: {total}/{total}")
        self.current_index = total
        self.result_text.see(tk.END)


def main():
    """主函数"""
    root = tk.Tk()
    app = DBQueryGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()

