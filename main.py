"""
主入口 - Gelbooru 图片下载器
使用方法: python main.py [模式]

架构说明:
- 主线程负责所有文件写入操作
- 工作线程只负责下载和收集数据
- 通过返回JSON数据避免并发冲突
- 缩略图生成使用独立单线程池异步处理
"""
import sys
import os
import time
import queue
import concurrent.futures
from core import (
    Regex, LoggerManager, WebClient, DatabaseManager, format_size, load_tag_mapping
)
import datetime
import glob
from operator import itemgetter
import set_tag
from set_tag import writefile, readfile
import sampletag
from downloader import down_single, down_batch_mode3_queue, set_sample_executor
from core import config, get_database, load_tag_mapping

# 全局缩略图线程池（单线程，避免PIL并发问题）
sample_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix='sample')

# 设置到downloader模块
set_sample_executor(sample_executor)


def write_failed_records(failed_records):
    """写入失败记录"""
    if not failed_records:
        return
    
    with open(config['path']['failed'], 'a', encoding='utf-8') as f:
        for record in failed_records:
            f.write(f"{record['tag']}|{record['url']}|{record['time']}|"
                   f"{record['id']}|{record['filename']}|{record['tags']}\n")


def write_tag_time(tag_time_dict):
    """
    写入downtag文件（支持4个历史时间戳）
    
    新格式: tag {num}: {tag} |time1: {time}|time2: {time}|time3: {time}|time4: {time}
    - time1: 最新的下载时间
    - time2-4: 历史下载时间（依次向后推移）
    - 如果新的time1和当前time1相同，不更新
    - 按time1倒序排列（最新在最下，number最小）
    - 使用 tag-replace.txt 将 replace_tag 转换为 original_tag
    """
    if not tag_time_dict:
        return
    
    file_path = config['path']['downtag']
    DEFAULT_TIME = '2000-01-01 00:00:00'
    
    # 加载 tag 映射关系（replace_tag -> original_tag）
    tag_mapping = load_tag_mapping(reverse=True)
    
    # 转换 tag_time_dict 中的 replace_tag 为 original_tag
    normalized_tag_time = {}
    for tag, time_val in tag_time_dict.items():
        # 如果是 replace_tag，转换为 original_tag
        original_tag = tag_mapping.get(tag, tag)
        normalized_tag_time[original_tag] = time_val
    
    # 读取现有数据（支持新旧两种格式）
    tag_times = {}  # {tag_name: [time1, time2, time3, time4]}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or 'time' not in line:
                    continue
                
                # 解析tag名
                if ': ' not in line:
                    continue
                parts = line.split(': ', 1)
                if len(parts) != 2:
                    continue
 # 解析时间（支持新旧格式）
                if '|time1:' in line:
                    # 新格式: |time1: xxx|time2: xxx|time3: xxx|time4: xxx
                    # 一次性解析完成
                    tag_name = parts[1].split('|')[0].strip()
                    time_parts = line.split('|')
                    times = []
                    for part in time_parts:
                        if 'time' in part and ':' in part:
                            time_str = part.split(':', 1)[1].strip()
                            if len(time_str) == 19:  # YYYY-MM-DD HH:MM:SS
                                times.append(time_str)
                    # 补齐到4个时间戳
                    times = (times + [DEFAULT_TIME] * 4)[:4]
                    tag_times[tag_name] = times
                
                elif 'time:' in line:
                    # 旧格式: time: xxx
                    time_str = line.split('time:', 1)[1].strip()
                    tag_name = parts[1].split('time:')[0].strip()
                    tag_times[tag_name] = [time_str, DEFAULT_TIME, DEFAULT_TIME, DEFAULT_TIME]
    except FileNotFoundError:
        pass  # 文件不存在，使用空字典
    
    # 更新新的时间
    def parse_time(time_str):
        return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    
    for tag, new_time in normalized_tag_time.items():
        if tag not in tag_times:
            # 新tag，初始化
            tag_times[tag] = [new_time, DEFAULT_TIME, DEFAULT_TIME, DEFAULT_TIME]
        else:
            current_times = tag_times[tag]
            # 如果新时间和time1相同，不更新
            if new_time != current_times[0]:
                # 新时间不同，向后推移
                tag_times[tag] = [
                    new_time,           # time1: 新时间
                    current_times[0],   # time2: 原time1
                    current_times[1],   # time3: 原time2
                    current_times[2]    # time4: 原time3（原time4丢弃）
                ]
    
    # 按time1排序（倒序，最新在最下）
    entries = []
    for tag, times in tag_times.items():
        time1_dt = parse_time(times[0])
        entries.append((tag, time1_dt, times))
    
    entries.sort(key=itemgetter(1), reverse=False)
    
    # 写入新格式
    content = []
    for idx, (tag, _, times) in enumerate(entries, 1):
        entry_id = len(entries) - idx + 1
        time_str = f"|time1: {times[0]}|time2: {times[1]}|time3: {times[2]}|time4: {times[3]}"
        line = f"tag {entry_id:4}: {tag.ljust(50)} {time_str}\n"
        content.append(line)
    
    with open(file_path, 'w') as f:
        f.writelines(content)



def print_summary_statistics(total_downloaded, total_failed, total_size):
    """
    打印汇总统计信息
    
    Args:
        total_downloaded: 总下载成功数
        total_failed: 总失败数
        total_size: 总下载大小（字节）
    """
    if total_downloaded == 0 and total_failed == 0:
        print("\n=== 汇总统计 ===")
        print("本次运行未下载新图片")
        return
    
    print("\n" + "="*50)
    print("           汇总统计")
    print("="*50)
    
    # 计算总大小和平均大小
    if total_size > 0:
        if total_size < 1024 * 1024:
            # 小于1MB，用KB显示
            total_size_str = f"{total_size / 1024:.2f} Kb"
            avg_size_str = f"{total_size / total_downloaded / 1024:.2f} Kb" if total_downloaded > 0 else "0 Kb"
        elif total_size < 1024 * 1024 * 1024:
            # 小于1GB，用MB显示
            total_size_str = f"{total_size / 1024 / 1024:.2f} Mb"
            avg_size_str = f"{total_size / total_downloaded / 1024 / 1024:.2f} Mb" if total_downloaded > 0 else "0 Mb"
        else:
            # 大于1GB，用GB显示
            total_size_str = f"{total_size / 1024 / 1024 / 1024:.2f} Gb"
            avg_size_str = f"{total_size / total_downloaded / 1024 / 1024:.2f} Mb" if total_downloaded > 0 else "0 Mb"
        
        print(f"Total size: {total_size_str}  avg size: {avg_size_str}")
    
    print(f"Total download: {total_downloaded} failed: {total_failed}")
    
    if total_downloaded > 0:
        success_rate = (total_downloaded / (total_downloaded + total_failed)) * 100
        print(f"Success rate: {success_rate:.1f}%")
    
    print("="*50 + "\n")


def handle_result(result):
    """
    处理单个下载器返回的结果，统一写入文件
    
    注意: 日志和Gelbooru/{tag}/tags.txt已由线程实时写入（各自独立文件，无冲突）
    
    Args:
        result: Downloader返回的字典
    """
    # 1. 写入失败记录
    if result.get('failed_records'):
        write_failed_records(result['failed_records'])
    
    # 2. 下载完成，删除数据库记录
    if result.get('delete_tag'):
        set_tag.delete_tagjson(result['tag'])
    
    # 3. 设置为完成
    if result.get('set_input_done'):
        set_tag.set_input_done(result['set_input_done'])
    
    # 6. 删除启动文件
    if result.get('remove_startfile') and os.path.exists(result.get('remove_startfile')):
        os.remove(result['remove_startfile'])
    
    # 7. 添加过期tag
    if result.get('expire_tags'):
        for expire_tag in result['expire_tags']:
            set_tag.add_expire_tag(expire_tag)
    
    # 注意：缩略图已在downloader中异步提交，此处无需处理

def _run_download_mode(status_filter, mode_name):
    """
    通用下载模式（mode 1/2 公共逻辑）
    
    Args:
        status_filter: 状态过滤器（list）
        mode_name: 模式名称（用于日志）
    """
    from core import get_database
    
    try:
        # 初始化
        set_tag.add_folder_tag()
        set_tag.init_input()
        
        tags_config = set_tag.read_tagjson()
        
        # 筛选任务
        tasks = [(tag, info) for tag, info in tags_config.items() 
                 if info['status'] in status_filter]
        
        if not tasks:
            print(f"没有需要{mode_name}的标签")
            return
        
        print(f"准备{mode_name} {len(tasks)} 个标签")
        
        # 使用6线程并发下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(down_single, tag, tag_config): tag 
                      for tag, tag_config in tasks}
            
            # 收集所有结果
            all_tag_time = {}
            total_downloaded = 0
            total_failed = 0
            total_size = 0
            
            for future in concurrent.futures.as_completed(futures):
                tag = futures[future]
                try:
                    result = future.result()
                    
                    # 统一处理结果（主线程写入）
                    handle_result(result)
                    
                    # 合并tag_time
                    if result.get('tag_time'):
                        all_tag_time.update(result['tag_time'])
                    
                    # 收集统计数据
                    stats = result.get('statistics', {})
                    total_downloaded += stats.get('downloaded', 0)
                    total_failed += stats.get('failed', 0)
                    total_size += stats.get('total_size', 0)
                    
                    print(f"✓ {tag} 完成")
                except Exception as e:
                    print(f"✗ {tag} 出错: {e}")
            
            # 最后统一写入tag_time
            if all_tag_time:
                write_tag_time(all_tag_time)
            
            # 打印汇总统计
            print_summary_statistics(total_downloaded, total_failed, total_size)
    
    finally:
        # 关闭数据库连接
        try:
            get_database().close_all_connections()
        except Exception as e:
            print(f"⚠️  关闭数据库连接失败: {e}")


def mode_1():
    """
    模式1: 下载新标签（支持动态添加 + 自动恢复中断）
    
    特点：
    - 下载 status=0（新tag）和 status=1（中断的tag）
    - 异常中断后自动恢复，无需手动切换模式
    - 每完成一个tag后立即标记done
    - 动态扫描input.txt，实时添加新tag
    - 自动清理孤立的 .start 文件
    """
    from core import get_database
    import os
    import glob
    
    try:
        # 初始化
        set_tag.add_folder_tag()
        set_tag.init_input()
        
        tags_config = set_tag.read_tagjson()
        
        # 清理孤立的 .start 文件
        new_path = config['path']['new']
        start_files = glob.glob(os.path.join(new_path, '*.start'))
        
        if start_files:
            # 获取所有活跃的 tag（转换为文件名格式）
            active_tags = set()
            for tag in tags_config.keys():
                replace_tag = tag.replace('/', '_').replace('\\', '_')
                active_tags.add(f'zzz{replace_tag}.start')
                active_tags.add(f'{tag}.start')
                active_tags.add(f'zzztag{replace_tag}.start')
            
            # 清理不属于活跃 tag 的 .start 文件
            cleaned = 0
            for start_file in start_files:
                basename = os.path.basename(start_file)
                if basename not in active_tags:
                    try:
                        os.remove(start_file)
                        cleaned += 1
                    except Exception as e:
                        print(f"⚠️  清理启动文件失败 {basename}: {e}")
            
            if cleaned > 0:
                print(f"✓ 清理了 {cleaned} 个孤立的启动文件")
        
        # 筛选初始任务（status=0 新tag + status=1 中断tag）
        initial_tasks = [(tag, info) for tag, info in tags_config.items() 
                        if info['status'] in [0, 1]]
        
        if not initial_tasks:
            print("没有需要下载的标签")
            return
        
        # 统计新tag和中断tag
        new_count = sum(1 for _, info in initial_tasks if info['status'] == 0)
        interrupted_count = sum(1 for _, info in initial_tasks if info['status'] == 1)
        
        if interrupted_count > 0:
            print(f"开始下载 {len(initial_tasks)} 个标签（{new_count} 个新标签 + {interrupted_count} 个恢复中断）")
        else:
            print(f"开始下载 {new_count} 个新标签")
        
        # 已处理的tag集合（避免重复）
        processed_tags = set()
        
        # 创建任务队列
        task_queue = queue.Queue()
        for tag, tag_config in initial_tasks:
            task_queue.put((tag, tag_config))
            processed_tags.add(tag)
        
        # 统计数据
        all_tag_time = {}
        total_downloaded = 0
        total_failed = 0
        total_size = 0
        completed_count = 0
        
        # 使用6线程并发下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            active_futures = {}
            
            # 提交初始任务（最多6个）
            while not task_queue.empty() and len(active_futures) < 6:
                tag, tag_config = task_queue.get()
                future = executor.submit(down_single, tag, tag_config)
                active_futures[future] = tag
            
            # 动态处理任务
            while active_futures:
                # 等待任何一个任务完成（超时1秒）
                done_futures = []
                try:
                    for future in concurrent.futures.as_completed(active_futures.keys(), timeout=1):
                        done_futures.append(future)
                except concurrent.futures.TimeoutError:
                    # 超时：没有任务完成，继续等待下一轮
                    # 但仍然检查新tag（每秒扫描一次）
                    pass
                
                # 处理完成的任务
                for future in done_futures:
                    tag = active_futures.pop(future)
                    completed_count += 1
                    
                    try:
                        result = future.result()
                        
                        # 统一处理结果（主线程写入）
                        handle_result(result)
                        
                        # 合并tag_time
                        if result.get('tag_time'):
                            all_tag_time.update(result['tag_time'])
                        
                        # 收集统计数据
                        stats = result.get('statistics', {})
                        total_downloaded += stats.get('downloaded', 0)
                        total_failed += stats.get('failed', 0)
                        total_size += stats.get('total_size', 0)
                        
                        print(f"✓ [{completed_count}] {tag} 完成")
                        
                        # 立即标记done（避免下次重新读取）
                        if result.get('set_input_done'):
                            set_tag.set_input_done(result['set_input_done'])
                        
                    except Exception as e:
                        print(f"✗ {tag} 出错: {e}")
                
                # 每次循环都检查新tag（不管是否有任务完成）
                new_tags = _scan_new_tags(processed_tags)
                for new_tag, new_config in new_tags:
                    task_queue.put((new_tag, new_config))
                    processed_tags.add(new_tag)
                    print(f"🆕 发现新标签: {new_tag}")
                
                # 补充新任务到线程池（保持6个并发）
                while not task_queue.empty() and len(active_futures) < 6:
                    tag, tag_config = task_queue.get()
                    future = executor.submit(down_single, tag, tag_config)
                    active_futures[future] = tag
            
            # 最后统一写入tag_time
            if all_tag_time:
                write_tag_time(all_tag_time)
            
            # 打印汇总统计
            print_summary_statistics(total_downloaded, total_failed, total_size)
    
    finally:
        # 关闭数据库连接
        try:
            get_database().close_all_connections()
        except Exception as e:
            print(f"⚠️  关闭数据库连接失败: {e}")


def _scan_new_tags(processed_tags):
    """
    扫描input.txt，查找新增的tag
    
    Args:
        processed_tags: 已处理的tag集合
    
    Returns:
        list: [(tag, config), ...] 新tag列表
    """
    new_tags = []
    
    try:
        input_lines = set_tag.readfile(config['path']['input'])
        
        for line in input_lines:
            line = line.strip()
            if not line or line.startswith('TAG') or line.startswith('done '):
                continue
            
            # 解析标签配置
            parts = line.split()
            if not parts:
                continue
            
            tag_name = parts[0]
            
            # 跳过已处理的tag
            if tag_name in processed_tags:
                continue
            
            # 补全参数：tag [endpage] [start_pic] [end_pic]
            if len(parts) == 1:
                parts.extend(['1', '0', '0'])
            elif len(parts) == 2:
                parts.extend(['0', '0'])
            elif len(parts) == 3:
                parts.append('0')
            
            # 构建配置
            tag_config = {
                'startpage': 1,
                'endpage': int(parts[1]),
                'start_pic': int(parts[2]),
                'end_pic': str(parts[3]),
                'status': 0
            }
            
            # 添加到数据库
            db = get_database()
            db.init_tag_progress(
                tag=tag_name,
                endpage=tag_config['endpage'],
                start_pic=tag_config['start_pic'],
                end_pic=tag_config['end_pic'],
                status=0
            )
            
            new_tags.append((tag_name, tag_config))
    
    except Exception as e:
        print(f"扫描新标签失败: {e}")
    
    return new_tags


def mode_3():
    """模式3: 下载所有旧标签（队列模式 - 动态负载均衡）"""
    
    try:
        print("\n=== Mode 3: Download All Old Tags ===\n")
        
        # 1. 初始化标签列表
        set_tag.add_folder_tag()
        set_tag.init_input(1)
        set_tag.add_dead_tag()
        taglist = set_tag.read_tags()
        
        if not taglist:
            print("没有可下载的标签")
            return
        
        print(f"总标签数: {len(taglist)}")
        
        # 2. 检查运行中的任务
        end_files = glob.glob(os.path.join(config['path']['new'], "*.start"))
        running = len(end_files)
        workers = 6
        
        if running >= workers:
            print(f"已有{running}个任务运行中，无法启动新任务")
            return
        
        # 3. 计算可用线程数
        available = workers - running
        print(f"运行中任务: {running}, 可用线程: {available}/{workers}")
        
        # 4. 创建任务队列并填充
        task_queue = queue.Queue()
        for tag in taglist:
            task_queue.put(tag)
        
        # 5. 添加结束标记（每个线程一个None）
        for _ in range(available):
            task_queue.put(None)
        
        print(f"任务队列已创建: {len(taglist)} 个tag\n")
        
        # 6. 数据收集变量
        all_tag_time = {}
        all_done_tags = []
        total_downloaded = 0
        total_failed = 0
        total_size = 0
        
        start_time = time.time()
        
        # 7. 启动工作线程
        with concurrent.futures.ThreadPoolExecutor(max_workers=available) as executor:
            futures = {executor.submit(down_batch_mode3_queue, task_queue, i+1): i+1 
                      for i in range(available)}
            
            # 8. 收集结果
            for future in concurrent.futures.as_completed(futures):
                offset = futures[future]
                try:
                    result = future.result()
                    
                    # 统一处理结果
                    handle_result(result)
                    
                    # 合并数据
                    if result.get('tag_time'):
                        all_tag_time.update(result['tag_time'])
                    
                    if result.get('done_tags'):
                        all_done_tags.extend(result['done_tags'])
                    
                    # 收集统计数据
                    stats = result.get('statistics', {})
                    total_downloaded += stats.get('downloaded', 0)
                    total_failed += stats.get('failed', 0)
                    total_size += stats.get('total_size', 0)
                    
                    done_count = len(result.get('done_tags', []))
                    print(f"✓ 线程{offset} 完成: {done_count} 个tag")
                    
                except Exception as e:
                    print(f"✗ 线程{offset} 出错: {e}")
        
        # 9. 等待所有任务完成
        task_queue.join()
        
        elapsed_minutes = (time.time() - start_time) / 60
        
        # 10. 统一写入（主线程）
        if all_tag_time:
            write_tag_time(all_tag_time)
            print(f"\n已更新 {len(all_tag_time)} 个tag到 downtag.txt")
        
        if all_done_tags:
            set_tag.add_tags(all_done_tags)
        print(f"已添加 {len(all_done_tags)} 个tag到 tags.txt")
    
        # 11. 输出汇总统计
        print(f"\n{'='*50}")
        print(f"  总耗时: {elapsed_minutes:.1f} 分钟")
        # 计算下载大小字符串
        if total_size < 1024 * 1024:
            total_size_str = f"{total_size / 1024:.2f} KB"
        elif total_size < 1024 * 1024 * 1024:
            total_size_str = f"{total_size / 1024 / 1024:.2f} MB"
        else:
            total_size_str = f"{total_size / 1024 / 1024 / 1024:.2f} GB"
        print(f"  已处理: {len(all_done_tags)}/{len(taglist)} 个tag 下载数量 {total_downloaded} 下载大小 {total_size_str}")
        print_summary_statistics(total_downloaded, total_failed, total_size)
        print(f"{'='*50}\n")
    
    finally:
        # 关闭数据库连接
        try:
            get_database().close_all_connections()
        except Exception as e:
            print(f"⚠️  关闭数据库连接失败: {e}")


def mode_4():
    """模式4: 清理已完成记录"""
    set_tag.del_input_done()
    print("已完成标签清理完毕")


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("使用方法: python main.py [1|3|4]")
        print("  1 - 下载新标签（自动恢复中断）")
        print("  3 - 下载所有旧标签")
        print("  4 - 清理已完成记录")
        return
    
    mode = sys.argv[1]
    
    try:
        if mode == '1':
            mode_1()
        elif mode == '3':
            mode_3()
        elif mode == '4':
            mode_4()
        else:
            print(f"未知模式: {mode}")
            print("可用模式: 1（新标签+恢复中断）, 3（所有旧标签）, 4（清理）")
    finally:
        # 等待所有缩略图任务完成
        print("\n等待缩略图生成完成...")
        sample_executor.shutdown(wait=True)
        print("所有任务完成")


if __name__ == '__main__':
    main()

