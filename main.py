"""
主入口 - Gelbooru 图片下载器
使用方法: python main.py [模式]

架构说明:
- 主线程负责所有文件写入操作
- 工作线程只负责下载和收集数据
- 通过返回JSON数据避免并发冲突
- 缩略图生成在每个tag下载完成后同步执行
"""
import sys
import os
import time
import queue
import atexit
import concurrent.futures
import datetime
import glob
from operator import itemgetter
import set_tag
from downloader import down_single, down_batch_mode3_queue
from core import config, get_database, load_tag_mapping, format_size


def _cleanup_on_exit():
    """程序退出时清理资源"""
    # 关闭数据库连接
    try:
        get_database().close_all_connections()
    except Exception:
        pass

# 注册退出清理钩子
atexit.register(_cleanup_on_exit)


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
    
    # 使用 format_size 统一格式化
    if total_size > 0:
        total_size_str = format_size(total_size)
        avg_size_str = format_size(total_size // total_downloaded) if total_downloaded > 0 else "0 B"
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
    db = get_database()
    
    # 1. 写入失败记录（同时写入txt和数据库）
    if result.get('failed_records'):
        write_failed_records(result['failed_records'])
        # 同时写入数据库
        for record in result['failed_records']:
            db.add_failed_download(
                record['tag'], record['url'], record['time'],
                record['id'], record['filename'], record['tags']
            )
    
    # 2. 写入下载成功的图片记录到数据库
    if result.get('downloaded_files'):
        for file_info in result['downloaded_files']:
            db.add_picture(file_info)
    
    # 3. 更新进度状态（中断恢复用）
    if result.get('status_updates'):
        for tag, update_info in result['status_updates'].items():
            set_tag.update_tagjson(tag, update_info['config'])
    
    # 4. 下载完成，删除数据库进度记录
    if result.get('delete_tag'):
        set_tag.delete_tagjson(result['tag'])
    
    # 5. 设置为完成
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


def _run_batch_queue_mode(mode_name, worker_func, result_handler=None, collect_tag_time=False):
    """
    通用批量队列处理模式（Mode 3/6/7 公共逻辑）
    
    Args:
        mode_name: 模式名称（用于日志）
        worker_func: 工作函数 (task_queue, offset) -> result
        result_handler: 可选的结果处理函数 (result, stats_collector) -> None
        collect_tag_time: 是否收集 tag_time（Mode 3需要）
    
    Returns:
        dict: 汇总统计结果
    """
    try:
        print(f"\n=== {mode_name} ===\n")
        
        # 1. 初始化标签列表
        set_tag.add_folder_tag()
        set_tag.init_input(1)
        set_tag.add_dead_tag()
        taglist = set_tag.read_tags()
        
        if not taglist:
            print("没有可处理的标签")
            return {}
        
        print(f"总标签数: {len(taglist)}")
        
        # 2. 检查运行中的任务
        end_files = glob.glob(os.path.join(config['path']['new'], "*.start"))
        running = len(end_files)
        workers = 6
        
        if running >= workers:
            print(f"已有{running}个任务运行中，无法启动新任务")
            return {}
        
        # 3. 计算可用线程数
        available = workers - running
        print(f"运行中任务: {running}, 可用线程: {available}/{workers}")
        
        # 4. 创建任务队列并填充
        task_queue = queue.Queue()
        for tag in taglist:
            task_queue.put(tag)
        
        # 5. 添加结束标记
        for _ in range(available):
            task_queue.put(None)
        
        print(f"任务队列已创建: {len(taglist)} 个tag\n")
        
        # 6. 数据收集
        stats_collector = {
            'all_tag_time': {},
            'all_done_tags': [],
            'total_downloaded': 0,
            'total_failed': 0,
            'total_size': 0,
            'total_added': 0,
            'total_updated': 0,
            'total_skipped': 0,
            'total_not_found': 0,
        }
        
        start_time = time.time()
        
        # 7. 启动工作线程
        with concurrent.futures.ThreadPoolExecutor(max_workers=available) as executor:
            futures = {executor.submit(worker_func, task_queue, i+1): i+1 
                      for i in range(available)}
            
            # 8. 收集结果
            for future in concurrent.futures.as_completed(futures):
                offset = futures[future]
                try:
                    result = future.result()
                    
                    # 通用结果处理
                    if result_handler:
                        result_handler(result, stats_collector)
                    else:
                        handle_result(result)
                    
                    # 合并tag_time（Mode 3）
                    if collect_tag_time and result.get('tag_time'):
                        stats_collector['all_tag_time'].update(result['tag_time'])
                    
                    if result.get('done_tags'):
                        stats_collector['all_done_tags'].extend(result['done_tags'])
                    
                    # 收集统计数据
                    stats = result.get('statistics', {})
                    stats_collector['total_downloaded'] += stats.get('downloaded', 0)
                    stats_collector['total_failed'] += stats.get('failed', 0)
                    stats_collector['total_size'] += stats.get('total_size', 0)
                    stats_collector['total_added'] += stats.get('added', 0)
                    stats_collector['total_updated'] += stats.get('updated', 0)
                    stats_collector['total_skipped'] += stats.get('skipped', 0)
                    stats_collector['total_not_found'] += stats.get('not_found', 0)
                    
                    done_count = result.get('processed_count', len(result.get('done_tags', [])))
                    interrupted_msg = " (interrupted)" if result.get('interrupted') else ""
                    print(f"✓ 线程{offset} 完成: {done_count} 个tag{interrupted_msg}")
                    
                except Exception as e:
                    print(f"✗ 线程{offset} 出错: {e}")
        
        # 9. 清空队列中可能残留的任务
        remaining = 0
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
                remaining += 1
            except:
                break
        if remaining > 0:
            print(f"⚠ 清理了 {remaining} 个未处理的队列任务")
        
        stats_collector['elapsed_minutes'] = (time.time() - start_time) / 60
        stats_collector['total_tags'] = len(taglist)
        
        return stats_collector
        
    finally:
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
    
    stats = _run_batch_queue_mode(
        mode_name="Mode 3: Download All Old Tags",
        worker_func=down_batch_mode3_queue,
        result_handler=lambda r, s: handle_result(r),
        collect_tag_time=True
    )
    
    if not stats:
        return
    
    # Mode 3 特有: 统一写入 tag_time 和 done_tags
    if stats['all_tag_time']:
        write_tag_time(stats['all_tag_time'])
        print(f"\n已更新 {len(stats['all_tag_time'])} 个tag到 downtag.txt")
    
    if stats['all_done_tags']:
        set_tag.add_tags(stats['all_done_tags'])
        print(f"已添加 {len(stats['all_done_tags'])} 个tag到 tags.txt")
    
    # 输出汇总日志
    def print_summary_log(msg):
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{current_time} | {msg}")
    
    total_downloaded = stats['total_downloaded']
    total_failed = stats['total_failed']
    total_size = stats['total_size']
    
    total_size_str = ""
    if total_downloaded > 0:
        total_size_str = format_size(total_size)
        avg_size_str = format_size(total_size // total_downloaded)
        print_summary_log(f'Total size: {total_size_str}  avg size: {avg_size_str}')
        print_summary_log(f'Total download: {total_downloaded} failed: {total_failed}')
    
    # 输出 expired tag 统计
    nulltag_count = 0
    nulltag_path = config['path'].get('nulltag', config['path'].get('deadtag'))
    if nulltag_path and os.path.exists(nulltag_path):
        with open(nulltag_path, 'r') as fd:
            nulltag_count = sum(1 for _ in fd)
    print_summary_log(f'expired tag: {nulltag_count}')
    
    print_summary_log(f'End tags:{len(stats["all_done_tags"])}')
    print_summary_log('End')
    
    # 输出汇总统计
    print(f"\n{'='*50}")
    print(f"  总耗时: {stats['elapsed_minutes']:.1f} 分钟")
    print(f"  已处理: {len(stats['all_done_tags'])}/{stats['total_tags']} 个tag")
    print(f"  下载数量: {total_downloaded}  失败: {total_failed}")
    if total_downloaded > 0:
        print(f"  下载大小: {total_size_str}")
    print(f"{'='*50}\n")


def mode_4():
    """模式4: 清理已完成记录"""
    set_tag.del_input_done()
    print("已完成标签清理完毕")


def mode_5(old_tag: str, new_tag: str):
    """
    模式5: 修改图片的tag_name
    
    用于处理tag名称变更的情况，例如艺术家改名
    
    Args:
        old_tag: 旧的tag名称
        new_tag: 新的tag名称
    
    更新内容:
        - tag_name: old_tag -> new_tag
        - file_path: 替换路径中的old_tag为new_tag
        - pic_tags: 追加new_tag，旧tag加_old后缀
    """
    from core import get_database
    
    print(f"\n=== Mode 5: 修改图片tag_name ===")
    print(f"  旧tag: {old_tag}")
    print(f"  新tag: {new_tag}\n")
    
    db = get_database()
    gelbooru_path = config['path']['Gelbooru']
    
    try:
        # 检查旧tag是否存在
        existing = db.get_pictures_by_tag(old_tag)
        if not existing:
            print(f"❌ 数据库中没有找到tag: {old_tag}")
            return
        
        print(f"找到 {len(existing)} 张图片需要更新")
        
        # 执行更新
        updated_count = db.update_picture_tag_name(old_tag, new_tag, gelbooru_path)
        
        print(f"✓ 成功更新 {updated_count} 条记录")
        print(f"\n注意: 请手动将文件夹从 {old_tag} 重命名为 {new_tag}")
        print(f"  路径: {gelbooru_path}\\{old_tag} -> {gelbooru_path}\\{new_tag}")
        
    except Exception as e:
        print(f"❌ 更新失败: {e}")
    finally:
        try:
            db.close_all_connections()
        except Exception:
            pass


def mode_6():
    """模式6: 更新图片信息（Mode 3变种，不下载只更新DB）"""
    from downloader import update_batch_mode6_queue
    
    stats = _run_batch_queue_mode(
        mode_name="Mode 6: 更新图片信息",
        worker_func=update_batch_mode6_queue
    )
    
    if not stats:
        return
    
    # 输出汇总统计
    print(f"\n{'='*50}")
    print(f"  总耗时: {stats['elapsed_minutes']:.1f} 分钟")
    print(f"  新增记录: {stats['total_added']}")
    print(f"  更新记录: {stats['total_updated']}")
    print(f"  跳过(本地无文件): {stats['total_skipped']}")
    print(f"{'='*50}\n")


def mode_7():
    """模式7: 从本地tags.txt导入图片信息到DB（不联网）"""
    from downloader import update_batch_mode7_queue
    
    stats = _run_batch_queue_mode(
        mode_name="Mode 7: 从本地tags.txt导入图片信息",
        worker_func=update_batch_mode7_queue
    )
    
    if not stats:
        return
    
    # 输出汇总统计
    print(f"\n{'='*50}")
    print(f"  总耗时: {stats['elapsed_minutes']:.1f} 分钟")
    print(f"  新增记录: {stats['total_added']}")
    print(f"  跳过(已存在): {stats['total_skipped']}")
    print(f"  未找到(无tags.txt): {stats['total_not_found']}")
    print(f"{'='*50}\n")


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("使用方法: python main.py [1|3|4|5|6|7]")
        print("  1 - 下载新标签（自动恢复中断）")
        print("  3 - 下载所有旧标签")
        print("  4 - 清理已完成记录")
        print("  5 old_tag new_tag - 修改图片tag_name")
        print("  6 - 更新图片信息（不下载，联网获取）")
        print("  7 - 从本地tags.txt导入图片信息（不联网）")
        return
    
    mode = sys.argv[1]
    
    try:
        if mode == '1':
            mode_1()
        elif mode == '3':
            mode_3()
        elif mode == '4':
            mode_4()
        elif mode == '5':
            if len(sys.argv) < 4:
                print("Mode 5 需要两个参数: python main.py 5 old_tag new_tag")
                return
            mode_5(sys.argv[2], sys.argv[3])
        elif mode == '6':
            mode_6()
        elif mode == '7':
            mode_7()
        else:
            print(f"未知模式: {mode}")
            print("可用模式: 1, 3, 4, 5, 6, 7")
    finally:
        print("\n所有任务完成")


if __name__ == '__main__':
    main()
