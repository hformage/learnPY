import re
import os
from collections import defaultdict
from datetime import datetime, timedelta

def parse_size(size_str):
    size_str = size_str.strip()
    if 'Kb' in size_str:
        return float(size_str.replace(' Kb', '')) * 1024
    elif 'Mb' in size_str:
        return float(size_str.replace(' Mb', '')) * 1024 * 1024
    elif 'Gb' in size_str:
        return float(size_str.replace(' Gb', '')) * 1024 * 1024 * 1024
    return 0

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
    # 初始化数据结构
    daily_tags = defaultdict(set)  # 每天的tag数量
    daily_downloads = defaultdict(lambda: defaultdict(list))  # 每天每个tag的下载数量和大小
    current_tag = None

    with open(path, 'r', encoding='utf-8') as f:
        prev_line_was_tag = False
        for line in f:
            if not line.strip():
                continue

            # 解析时间和内容
            try:
                timestamp, content = line.strip().split(' | ', 1)
                date = timestamp.split()[0]  # YYYY-MM-DD
                if len(date) != 10:
                    continue
            except ValueError:
                continue

            # 跳过start和end相关的行
            if any(x in content.lower() for x in ['start', 'end', 'total size']):
                continue

            # 匹配Tag行
            tag_match = re.search(r'Tag\(.*?\): \d+ (.+)', content)
            if tag_match:
                tag_name = tag_match.group(1)
                if prev_line_was_tag:  # 如果上一行也是tag，说明前一个tag没有下载内容
                    daily_tags[date].add(current_tag)  # 添加前一个tag
                current_tag = tag_name
                prev_line_was_tag = True
                continue

            # 匹配下载行
            download_match = re.search(r'([^()]+)\(\d+\) \d+ [\d-]+ [\d:]+ \S+ ([\d.]+ (?:Kb|Mb|Gb))', content)
            if download_match and current_tag:
                if not any(x in content.lower() for x in ['skip', 'count:', 'total']):
                    tag_name = download_match.group(1).strip()
                    size_str = download_match.group(2)
                    size = parse_size(size_str)
                    daily_downloads[date][current_tag].append(size)
                    prev_line_was_tag = False

            # 如果是count行，说明一个tag的下载结束
            if 'count:' in content:
                daily_tags[date].add(current_tag)
                prev_line_was_tag = False
                
    return daily_tags, daily_downloads

def format_size(size):
    """格式化文件大小显示"""
    if size > 1024*1024*1024:
        return f"{size/1024/1024/1024:.2f} GB"
    elif size > 1024*1024:
        return f"{size/1024/1024:.2f} MB"
    else:
        return f"{size/1024:.2f} KB"

def print_stats(daily_tags, daily_downloads):
    """打印统计结果"""
    grand_total_files = 0
    grand_total_size = 0
    
    for date in sorted(daily_tags.keys()):
        # print(f"\n=== Date: {date} ===")
        # print(f"Total unique tags processed: {len(daily_tags[date])}")
        
        # # 打印每个tag的下载数量
        # if date in daily_downloads:
        #     print("\nDownloads by tag:")
        #     for tag, sizes in sorted(daily_downloads[date].items()):
        #         if sizes:  # 只显示有下载的tag
        #             total_size = sum(sizes)
        #             print(f"  {tag}: {len(sizes)} files, total size: {format_size(total_size)}")
        
        # 打印当天总下载
        total_files = sum(len(sizes) for sizes in daily_downloads[date].values())
        total_size = sum(sum(sizes) for sizes in daily_downloads[date].values())
        avg_size = total_size/total_files if total_files else 0
        print(f"{date}: {len(daily_tags[date])} | {total_files} | {format_size(total_size)} | {format_size(avg_size)}")
        
        # 累加到总计
        grand_total_files += total_files
        grand_total_size += total_size
    
    # # 打印总计
    # print("\n=== Grand Total ===")
    # print(f"Total files downloaded: {grand_total_files}")
    # print(f"Total size downloaded: {format_size(grand_total_size)}")
    # print(f"Total unique tags across all days: {len(set.union(*[tags for tags in daily_tags.values()]))} tags")

def analyze_all_logs():
    """分析所有日志文件并合并结果"""
    # 初始化合并结果
    merged_tags = defaultdict(set)
    merged_downloads = defaultdict(lambda: defaultdict(list))
    
    # 处理所有日志文件
    for i in range(1, 7):
        log_file = f'F:\\Pic\\Gelbooru\\new\\taglog{i}.txt'
        if os.path.exists(log_file):
            #print(f"Processing {log_file}...")
            delete_log(log_file)  # 删除两个月前的记录
            daily_tags, daily_downloads = check_log(log_file)
            merged_tags, merged_downloads = merge_stats((merged_tags, merged_downloads), 
                                                      (daily_tags, daily_downloads))
    
    # 打印合并后的结果
    #print("\nCombined statistics for all log files:")
    print_stats(merged_tags, merged_downloads)

def delete_log(path):
    """删除指定天数之前的日志记录，使用变量 `delete_day` 作为阈值（以天为单位）。"""
    # 要删除多少天之前的记录（默认60天）
    delete_day = 60
    cutoff = datetime.now() - timedelta(days=delete_day)

    # 读取所有行
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 筛选保留的行：只保留日期在 cutoff 之后或无法解析日期的行
    kept_lines = []
    for line in lines:
        if not line.strip():  # 跳过空行
            continue
        if len(line.strip().split(' | ')[0]) != 19:
            continue
        try:
            # 先取行首第一个 token，通常为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
            first_token = line.strip().split()[0]
            # 支持仅日期或日期+时间两种格式
            try:
                line_dt = datetime.strptime(first_token, '%Y-%m-%d')
            except ValueError:
                # 如果第一 token 包含时间（例如 'YYYY-MM-DD HH:MM:SS'），尝试前 19 字符解析
                line_dt = datetime.strptime(line.strip()[:19], '%Y-%m-%d %H:%M:%S')

            # 保留在截止日期之后的行（包括同一天）
            if line_dt >= cutoff:
                kept_lines.append(line)
        except Exception:
            # 若解析失败（非日志行），保留该行以免丢失信息
            kept_lines.append(line)

    # 写回文件
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(kept_lines)

if __name__ == '__main__':
    analyze_all_logs()
