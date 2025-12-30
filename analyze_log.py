import re
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from contextlib import contextmanager

# 数据库路径配置
DB_PATH = r'F:\Pic\Gelbooru\new\gelbooru_metadata.db'
# 日志保留天数（删除多少天前的日志记录）
DEL_DAYS = 60

def parse_size(size_str):
    size_str = size_str.strip()
    if 'Kb' in size_str:
        return float(size_str.replace(' Kb', '')) * 1024
    elif 'Mb' in size_str:
        return float(size_str.replace(' Mb', '')) * 1024 * 1024
    elif 'Gb' in size_str:
        return float(size_str.replace(' Gb', '')) * 1024 * 1024 * 1024
    return 0

def format_size(size):
    """格式化文件大小显示"""
    if size > 1024*1024*1024:
        return f"{size/1024/1024/1024:.2f} GB"
    elif size > 1024*1024:
        return f"{size/1024/1024:.2f} MB"
    else:
        return f"{size/1024:.2f} KB"

@contextmanager
def get_db_cursor(db_path=DB_PATH):
    """获取数据库游标的上下文管理器"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def analyze_from_db(days=60):
    """从数据库读取下载统计（更准确）"""
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
        return
    
    # 先清理所有日志文件（删除DEL_DAYS天前的记录）
    for i in range(1, 7):
        log_file = f'F:\\Pic\\Gelbooru\\new\\taglog{i}.txt'
        if os.path.exists(log_file):
            delete_log(log_file, DEL_DAYS)
    
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    with get_db_cursor() as cursor:
        # 按日期统计
        cursor.execute("""
            SELECT 
                pic_date,
                COUNT(DISTINCT tag_name) as tag_count,
                COUNT(*) as file_count,
                SUM(file_size) as total_size
            FROM pictures
            WHERE pic_date >= ?
            GROUP BY pic_date
            ORDER BY pic_date
        """, (cutoff_date,))
        
        results = cursor.fetchall()
        
        if not results:
            print("没有找到符合条件的记录")
            return
        
        grand_total_files = 0
        grand_total_size = 0
        
        for row in results:
            date = row['pic_date']
            tag_count = row['tag_count']
            file_count = row['file_count']
            total_size = row['total_size'] or 0
            avg_size = total_size / file_count if file_count else 0
            
            print(f"{date}: {tag_count} | {file_count} | {format_size(total_size)} | {format_size(avg_size)}")
            
            grand_total_files += file_count
            grand_total_size += total_size
        
        # 打印总计
        print(f"Total files: {grand_total_files}")
        print(f"Total size : {format_size(grand_total_size)}")
        if grand_total_files:
            print(f"Avg size   : {format_size(grand_total_size / grand_total_files)}")

def merge_stats(stats1, stats2):
    """合并两个统计结果"""
    merged_tags = defaultdict(set)
    merged_downloads = defaultdict(lambda: defaultdict(list))
    
    # 合并tags
    for date in set(list(stats1[0].keys()) + list(stats2[0].keys())):
        merged_tags[date] = stats1[0][date].union(stats2[0][date])
    
    # 合并downloads
    for date in set(list(stats1[1].keys()) + list(stats2[1].keys())):
        for tag in set(list(stats1[1][date].keys()) + list(stats2[1][date].keys())):
            merged_downloads[date][tag].extend(stats1[1][date][tag])
            merged_downloads[date][tag].extend(stats2[1][date][tag])
    
    return merged_tags, merged_downloads

def check_log(path):
    """从日志文件解析统计（支持Mode 1/2/3格式）"""
    daily_tags = defaultdict(set)
    daily_downloads = defaultdict(lambda: defaultdict(list))
    current_tag = None
    prev_line_was_tag = False

    # 预编译正则表达式（提高性能）
    # Mode 1/2: i/total(page/endpage) tag file_counter date time filename size
    mode12_pattern = re.compile(
        r'(\d+/\d+\(\d+/\d+\))\s+(\S+)\s+\d+\s+([\d-]+)\s+[\d:]+\s+\S+\s+([\d.]+\s*(?:Kb|Mb|Gb))'
    )
    # Mode 3: tag(offset) file_counter date time filename size
    mode3_pattern = re.compile(
        r'([^()]+)\((\d+)\)\s+\d+\s+([\d-]+)\s+[\d:]+\s+\S+\s+([\d.]+\s*(?:Kb|Mb|Gb))'
    )
    # Tag行: Tag(xxx): number tag_name
    tag_pattern = re.compile(r'Tag\([^)]*\):\s*\d+\s+(.+)')
    
    # 跳过的关键字（合并判断提高效率）
    skip_keywords = ('start', 'end', 'total size', 'total time', 'total download')

    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # 解析时间和内容
                parts = line.split(' | ', 1)
                if len(parts) != 2:
                    continue
                
                timestamp, content = parts
                log_date = timestamp[:10]  # YYYY-MM-DD
                if len(log_date) != 10:
                    continue

                # 跳过特定行
                content_lower = content.lower()
                if any(kw in content_lower for kw in skip_keywords):
                    continue

                # 匹配Tag行
                tag_match = tag_pattern.search(content)
                if tag_match:
                    if prev_line_was_tag and current_tag:
                        daily_tags[log_date].add(current_tag)
                    current_tag = tag_match.group(1).strip()
                    prev_line_was_tag = True
                    continue

                # 匹配下载行
                m12 = mode12_pattern.search(content)
                if m12:
                    tag_name = m12.group(2)
                    size = parse_size(m12.group(4))
                    daily_downloads[log_date][tag_name].append(size)
                    daily_tags[log_date].add(tag_name)
                    prev_line_was_tag = False
                    continue
                
                m3 = mode3_pattern.search(content)
                if m3:
                    tag_name = m3.group(1).strip()
                    size = parse_size(m3.group(4))
                    daily_downloads[log_date][tag_name].append(size)
                    daily_tags[log_date].add(tag_name)
                    prev_line_was_tag = False
                    continue

                # count行标记tag结束
                if 'count:' in content_lower and current_tag:
                    daily_tags[log_date].add(current_tag)
                    prev_line_was_tag = False
    
    except FileNotFoundError:
        pass
                
    return daily_tags, daily_downloads

def print_stats(daily_tags, daily_downloads):
    """打印统计结果"""
    if not daily_tags:
        print("没有数据")
        return
    
    for date in sorted(daily_tags.keys()):
        tag_count = len(daily_tags[date])
        total_files = sum(len(sizes) for sizes in daily_downloads[date].values())
        total_size = sum(sum(sizes) for sizes in daily_downloads[date].values())
        avg_size = total_size / total_files if total_files else 0
        
        print(f"{date}: {tag_count} | {total_files} | {format_size(total_size)} | {format_size(avg_size)}")

def analyze_all_logs():
    """分析所有日志文件并合并结果"""
    merged_tags = defaultdict(set)
    merged_downloads = defaultdict(lambda: defaultdict(list))
    
    # 处理所有日志文件（taglog1.txt ~ taglog6.txt）
    for i in range(1, 7):
        log_file = f'F:\\Pic\\Gelbooru\\new\\taglog{i}.txt'
        if os.path.exists(log_file):
            delete_log(log_file, DEL_DAYS)  # 删除DEL_DAYS天前的记录
            daily_tags, daily_downloads = check_log(log_file)
            merged_tags, merged_downloads = merge_stats(
                (merged_tags, merged_downloads), 
                (daily_tags, daily_downloads)
            )
    
    print_stats(merged_tags, merged_downloads)

def delete_log(path, delete_days=60):
    """删除指定天数之前的日志记录"""
    cutoff = datetime.now() - timedelta(days=delete_days)
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        return
    
    kept_lines = []
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
        
        # 检查行格式: "YYYY-MM-DD HH:MM:SS | content"
        parts = line_stripped.split(' | ', 1)
        if len(parts) != 2 or len(parts[0]) != 19:
            continue
        
        try:
            line_dt = datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
            if line_dt >= cutoff:
                kept_lines.append(line)
        except ValueError:
            # 保留无法解析的行
            kept_lines.append(line)
    
    # 写回文件
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(kept_lines)

if __name__ == '__main__':
    import sys
    
    # 使用说明：
    # python analyze_log.py        -> 从数据库分析（默认60天）
    # python analyze_log.py 30     -> 从数据库分析最近30天
    # python analyze_log.py 1      -> 从日志文件分析
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == '1':
            #print("从日志文件分析:\n")
            analyze_all_logs()
        else:
            # 参数是天数
            try:
                days = int(arg)
                #print(f"从数据库分析最近 {days} 天的下载记录:\n")
                analyze_from_db(days)
            except ValueError:
                #print("无效参数，使用默认60天数据库分析\n")
                analyze_from_db(60)
    else:
        # 默认：从数据库分析60天
        #print("从数据库分析最近 60 天的下载记录:\n")
        analyze_from_db(60)
