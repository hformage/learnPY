"""
下载模块 - 核心下载逻辑
"""
import os
import time
import random
import queue
from win32file import CreateFile, SetFileTime, CloseHandle
from win32file import GENERIC_READ, GENERIC_WRITE, OPEN_EXISTING
from pywintypes import Time

# 导入配置和工具
from core import config, DOWN_SKIP_THRESHOLD, FILE_COUNT_CHECK_INTERVAL, RANDOM_DELAY_MIN, RANDOM_DELAY_MAX
from core import Regex, read_lines, get_max_file_number, format_size, get_database, WebClient, load_tag_mapping, parse_exauthor
import set_tag
import sampletag

def _run_sample_sync(tag_name):
    """同步执行缩略图生成任务（每个tag下载完成后立即执行并等待完成）"""
    if tag_name:
        try:
            sampletag.main(tag_name)
        except Exception as e:
            print(f"缩略图任务失败: {e}")


class Downloader:
    """下载器类"""
    
    def __init__(self):
        self.config = config
        self.web = WebClient(config['headers'])
        
        # 缓存常用路径和URL
        self.gelbooru_path = config['path']['Gelbooru']
        self.new_path = config['path']['new']
        self.base_url = config['url']
        
        # 初始化数据库连接
        self.db = get_database()
        
        # 读取排除规则
        self.extag = read_lines(config['path']['extag'])
        self.exauthor = read_lines(config['path']['exauthor'])
        self.expic = set(read_lines(config['path']['expic']))
        self.dead_tag = read_lines(config['path']['deadtag'])
        
        # 解析 exauthor 规则
        self.skip_tags, self.exclude_author_tags = parse_exauthor(self.exauthor)
        
        # 统计
        self.downed_cnt = 0
        self.failed_cnt = 0
        self.total_download_size = 0
        
        # 数据收集（返回给主线程）
   # 注意: Gelbooru/{tag}/tags.txt 由线程直接写入（每个tag独立文件，无冲突）
        self.result = {
            'logs': [],              # 日志记录列表
            'tag_time': {},          # tag时间记录
            'failed_records': [],    # 失败记录
            'status_updates': {},    # tagjson状态更新
            'expire_tags': [],       # 过期tag列表
            'downloaded_files': [],  # 已下载文件信息
            'statistics': {}         # 统计信息
        }
    
    def _normalize_tag(self, tag):
        """
        标准化标签名（替换特殊字符）
        
        同时记录 original_tag -> replace_tag 的映射关系到 tag-replace.txt
        """
        replace_tag = tag.replace(":", "_").replace(".", "_").replace("+", "_")
        
        # 如果 tag 被替换了，记录到文件
        if replace_tag != tag:
            try:
                tag_replace_path = config['path']['tag_replace']
                
                # 读取现有映射
                existing_mappings = load_tag_mapping()
                
                # 添加新映射
                if tag not in existing_mappings or existing_mappings[tag] != replace_tag:
                    existing_mappings[tag] = replace_tag
                    
                    # 写回文件
                    with open(tag_replace_path, 'w', encoding='utf-8') as f:
                        f.write('original_tag,replace_tag\n')
                        for orig, repl in sorted(existing_mappings.items()):
                            f.write(f'{orig},{repl}\n')
            except Exception as e:
                # 记录失败不影响主流程
                print(f"⚠️  记录tag映射失败: {e}")
        
        return replace_tag
    
    def init_single(self, tag):
        """初始化单标签下载"""
        self.tag = tag
        self.replace_tag = self._normalize_tag(tag)
        self.logpath = self.new_path + '\\z ' + self.replace_tag + '.txt'
        self.startfile = self.new_path + '\\zzz' + self.replace_tag + '.start'
        self.result['logpath'] = self.logpath
        self.result['tag'] = tag
        self.result['replace_tag'] = self.replace_tag
    
    def init_batch(self, offset=0):
        """初始化批量下载"""
        self.logpath = config['path']['taglog'] + str(offset) + '.txt'
        self.startfile = config['path']['startfile'] + str(offset) + '.start'
        self.result['logpath'] = self.logpath
        self.result['offset'] = offset
    
    def log(self, msg):
        """实时写入日志（每个线程独立logpath，无冲突风险）"""
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        formatted_msg = f"{current_time} | {msg}"
        
        # 立即写入日志文件
        with open(self.logpath, 'a', encoding='utf-8') as f:
            f.write(formatted_msg + '\n')
        
        # 打印到控制台
        print(formatted_msg)
    
    def _create_folder(self, path):
        """创建文件夹"""
        try:
            os.makedirs(path)
            self.log(f'{self.replace_tag} created')
        except FileExistsError:
            pass  # 文件夹已存在
    
    def _chk_start(self):
        """检查启动文件"""
        return os.path.exists(self.startfile)
    
    def _add_downloadtag(self, tag, tag_time):
        """添加下载tag时间记录"""
        if tag not in self.result['tag_time']:
            self.result['tag_time'][tag] = tag_time
        else:
            self.result['tag_time'][tag] = max(self.result['tag_time'][tag], tag_time)
    
    def _should_skip_tag(self, tag):
        """检查tag是否应该跳过（在exauthor中但没有逗号的tag）"""
        return tag in self.skip_tags
    
    def _exclude_url(self, tag):
        """构建排除URL"""
        url = ''
        for i in config['tag']['default_exclude_tags']:
            url += '+-' + i
        if tag in self.extag:
            for i in config['tag']['excludetag']:
                url += '+-' + i
        # 处理 exauthor 规则：添加排除的作者标签
        if tag in self.exclude_author_tags:
            for author in self.exclude_author_tags[tag]:
                url += '+' + author
        return url
    
    def _get_page_url(self, page):
        """获取分页URL"""
        return "" if page == 1 else f"&pid={42 * (page - 1)}"
    
    def _get_max_file_num(self):
        """获取最大文件序号"""
        return get_max_file_number(config['path']['new']) + 1
    
    def _extract_metadata(self, soup):
        """提取图片元数据"""
        try:
            pic_time = Regex.extract_posted_time(
                soup.find_all(string=Regex.POSTED_TIME)[0]
            )
            pic_id = Regex.extract_id_text(
                soup.find_all(string=Regex.ID_TEXT)[0]
            )
            pic_url = Regex.extract_pic_url(soup)
            pic_tags = Regex.extract_pic_tags(soup)
            pic_filename = Regex.extract_pic_filename(soup)
            pic_date = pic_time[:10] if pic_time else None
            return pic_time, pic_date, pic_id, pic_url, pic_tags, pic_filename
        except Exception:
            return None, None, None, None, None, None
    
    def _save_image(self, pic_data, file_content):
        """保存图片文件（实际写入），并收集元数据"""
        save_path = pic_data['save_path']
        new_path = pic_data['new_path']
        pic_time = pic_data['pic_time']
        
        # 保存到原始目录
        with open(save_path, 'wb') as f:
            f.write(file_content)
        
        # 设置文件时间
        cTime_t = time.localtime(time.mktime(time.strptime(pic_time, "%Y-%m-%d %H:%M:%S")))
        createTimes = Time(time.mktime(cTime_t))
        fh = CreateFile(save_path, GENERIC_READ | GENERIC_WRITE, 0, None, OPEN_EXISTING, 0, 0)
        SetFileTime(fh, createTimes, createTimes, createTimes)
        CloseHandle(fh)
        
        # 保存到new目录
        with open(new_path, 'wb') as fp:
            fp.write(file_content)
        
        # 收集文件信息（不写数据库，由主线程处理）
        self.result['downloaded_files'].append({
            'pic_id': pic_data['pic_id'],
            'tag_name': pic_data['tag_name'],
            'filename': pic_data['filename'],
            'new_filename': pic_data['new_filename'],
            'file_path': pic_data['file_path'],
            'file_size': pic_data['file_size'],
            'pic_url': pic_data['pic_url'],
            'pic_tags': pic_data['pic_tags'],
            'pic_time': pic_data['pic_time'],
            'pic_date': pic_data['pic_date']
        })
    
    def _write_failed(self, tag, pic_url, pic_time, pic_id, pic_filename, pic_tags):
        """记录失败（仅收集数据）"""
        # 收集失败记录
        self.result['failed_records'].append({
            'tag': tag,
            'url': pic_url,
            'time': pic_time,
            'id': pic_id,
            'filename': pic_filename,
            'tags': pic_tags
        })
    
    def download_single(self, tag, tag_config):
        """
        单标签下载（只负责下载和收集数据，不写入文件）
        
        Args:
            tag: 标签名
            tag_config: 标签配置 {'startpage', 'endpage', 'start_pic', 'end_pic', 'status'}
        
        Returns:
            dict: 包含所有需要写入的数据
        """
        self.init_single(tag)
        self.tag = tag
        self.tag_config = tag_config.copy()  # 复制一份避免修改原始数据
        
        # 检查是否应该跳过这个tag
        if self._should_skip_tag(tag):
            self.log(f'Skip tag in exauthor: {tag}')
            return self.result
        
        self.log('Start')
        
        # 立即创建启动文件（避免延迟）
        with open(self.startfile, 'w') as f:
            f.write('')
        
        # 初始化标签时间记录
        self._add_downloadtag(tag, '2000-01-01 00:00:00')
        
        # 收集状态更新（不直接写入）
        self.tag_config['status'] = 1
        self.result['status_updates'][tag] = {'status': 1, 'config': self.tag_config.copy()}
        
        sleep_time = random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
        time.sleep(sleep_time)
        
        # 创建保存目录
        path = os.path.join(self.gelbooru_path, self.replace_tag)
        self._create_folder(path)
        
        self.log(f"download page:({tag_config['startpage']}/{tag_config['endpage']}/{tag_config['start_pic']}/{tag_config['end_pic']}) Page url:{self.base_url+tag}")
        
        file_counter = self._get_max_file_num()
        start_time = time.time()
        base_tag_url = self.base_url + tag + self._exclude_url(tag)
        interrupted = False  # 是否被中断
        
        # 遍历页面
        for page in range(tag_config['startpage'], tag_config['endpage'] + 1):
            if not self._chk_start():
                interrupted = True
                # 保存当前页面进度
                self.tag_config['startpage'] = page
                self.result['status_updates'][tag] = {'status': 1, 'config': self.tag_config.copy()}
                break
            
            url = base_tag_url + self._get_page_url(page)
            soup = self.web.get_soup(url, retries=50)
            
            if not soup:
                break
            
            image_urls = self.web.get_image_list(soup)
            image_ids = self.web.get_image_ids(soup)
            
            if not image_urls:
                if page == 1 and tag not in self.dead_tag:
                    set_tag.add_expire_tag(tag)
                break
            
            self.log(f'page start: {page}/{tag_config["endpage"]} {url}')
            total_images = len(image_urls)
            self.log(f'Total get {total_images}')
            
            # 遍历图片
            for i, (img_url, img_id) in enumerate(zip(image_urls, image_ids), 1):
                # 更新当前进度（用于中断恢复）
                self.tag_config['startpage'] = page
                self.tag_config['start_pic'] = i
                
                # 检查是否被中断（与旧代码一致）
                if not self._chk_start():
                    interrupted = True
                    # 保存进度以便恢复
                    self.result['status_updates'][tag] = {'status': 1, 'config': self.tag_config.copy()}
                    break
                
                if i < tag_config['start_pic']:
                    continue
                
                if img_id in self.expic:
                    continue
                
                # 每5张重新检查文件计数
                if (i - tag_config['start_pic']) % FILE_COUNT_CHECK_INTERVAL == 0:
                    file_counter = self._get_max_file_num()
                
                # 获取详情页
                detail_soup = self.web.get_soup(img_url, retries=50)
                if not detail_soup:
                    continue
                
                pic_time, pic_date, pic_id, pic_url, pic_tags, pic_filename = self._extract_metadata(detail_soup)
                if not pic_filename:
                    continue
                
                # 记录最新的图片时间
                self._add_downloadtag(tag, pic_time)
                
                new_filename = f"{file_counter}_{self.replace_tag}_{pic_date}_{pic_id}.{pic_filename.split('.')[-1]}"
                original_save_path = os.path.join(self.gelbooru_path, self.replace_tag, pic_filename)
                new_save_path = os.path.join(self.new_path, new_filename)
                
                # 检查是否已存在
                if os.path.exists(original_save_path):
                    self.log(f'{i}/{total_images} skip {pic_filename} {pic_time}')
                    # 直接写入tag自己的tags.txt（每个线程处理不同tag，无冲突）
                    set_tag.update_tags(self.replace_tag, f"{tag}|{pic_time}|{pic_filename}|{pic_id}|{pic_tags}")
                else:
                    # 下载图片
                    image_content = self.web.download_image(pic_url, retries=50)
                    
                    if not image_content:
                        self.failed_cnt += 1
                        self.log(f'failed {tag} {pic_filename}')
                        self._write_failed(tag, pic_url, pic_time, pic_id, pic_filename, pic_tags)
                    else:
                        size = len(image_content)
                        self.total_download_size += size
                        
                        # 保存图片
                        image_metadata = {
                            'pic_id': pic_id,
                            'tag_name': self.replace_tag,
                            'filename': pic_filename,
                            'new_filename': new_filename,
                            'file_path': original_save_path,
                 'file_size': size,
                            'pic_url': pic_url,
                            'pic_tags': pic_tags,
                            'pic_time': pic_time,
                            'pic_date': pic_date,
                            'save_path': original_save_path,
                            'new_path': new_save_path
                        }
                        
                        self._save_image(image_metadata, image_content)
                        self.downed_cnt += 1
                        file_counter += 1
                        
                        formatted_size = format_size(size)
                        self.log(f'{i}/{total_images}({page}/{tag_config["endpage"]}) {tag} {file_counter-1} {pic_time} {pic_filename} {formatted_size}')
                        # 直接写入tag自己的tags.txt（每个线程处理不同tag，无冲突）
                        set_tag.update_tags(self.replace_tag, f"{tag}|{pic_time}|{pic_filename}|{pic_id}|{pic_tags}")
                
                # 检查是否到达终点
                if tag_config['end_pic'] != '0' and pic_id == tag_config['end_pic']:
                    self.result['set_input_done'] = tag  # 标记需要设置为完成
                    break
            
            # 如果被中断，跳出页面循环
            if interrupted:
                break
            
            self.tag_config['start_pic'] = 0
        
        # 整理tags.txt（去重排序）- 直接在线程中执行，每个tag独立文件无冲突
        set_tag.update_tags(self.replace_tag, None, 'u')
        
        # 只有正常完成（未中断）且启动文件存在时，才标记为done
        if not interrupted and self._chk_start():
            self.result['delete_tag'] = True
            self.result['remove_startfile'] = self.startfile
            if 'set_input_done' not in self.result:
                self.result['set_input_done'] = tag
        else:
            # 被中断，不标记done，保留状态以便恢复
            self.log(f'Interrupted, not marking as done')
        
        sleep_time = random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
        time.sleep(sleep_time)
        
        # 统计
        elapsed_minutes = round((time.time() - start_time) / 60)
        self.log(f'Total time: {elapsed_minutes} mins')
        
        if self.downed_cnt > 0:
            formatted_total_size = format_size(self.total_download_size)
            formatted_avg_size = format_size(self.total_download_size // self.downed_cnt)
            self.log(f'Total size: {formatted_total_size}  avg size: {formatted_avg_size}')
        
        self.log(f'Total download: {self.downed_cnt} failed: {self.failed_cnt}')
        
        # 收集统计信息
        self.result['statistics'] = {
            'downloaded': self.downed_cnt,
            'failed': self.failed_cnt,
            'total_size': self.total_download_size,
            'elapsed_minutes': elapsed_minutes
        }
        
        # 检查过期标签
        self._check_expire()
        
        self.log('End')
        
        # 同步执行缩略图任务（如果有下载）
        if self.downed_cnt > 0 and self.replace_tag:
            _run_sample_sync(self.replace_tag)
        
        # 返回所有收集的数据
        return self.result
    
    def _download_tag_batch(self, tag, offset, tag_index=0):
        """
        Mode 3 单个tag的批量下载逻辑（连续skip 2次停止）
        
        Args:
            tag: 标签名
            offset: 线程编号
            tag_index: 当前是第几个tag（用于日志输出）
        
        Returns:
            bool: 是否成功下载
        """
        self.tag = tag
        self.replace_tag = self._normalize_tag(tag)
        
        # 初始化
        self._add_downloadtag(tag, '2000-01-01 00:00:00')
        skip_count = 0  # 连续skip计数
        page = 0
        downloaded_this_tag = 0
        total_size_this_tag = 0
        
        # 创建保存目录
        path = os.path.join(self.gelbooru_path, self.replace_tag)
        self._create_folder(path)
        
        file_counter = self._get_max_file_num()
        base_tag_url = self.base_url + tag + self._exclude_url(tag)
        
        while True:
            page += 1
            url = base_tag_url + self._get_page_url(page)
            soup = self.web.get_soup(url, retries=50)
            
            if not soup:
                break
            
            image_urls = self.web.get_image_list(soup)
            image_ids = self.web.get_image_ids(soup)
            
            if not image_urls:
                if page == 1 and tag not in self.dead_tag:
                    self.result['expire_tags'].append(tag)
                break
            
            total_images = len(image_urls)
            
            # 遍历图片
            for i, (img_url, img_id) in enumerate(zip(image_urls, image_ids), 1):
                if img_id in self.expic:
                    self.log(f'skip ----{img_id}----')
                    continue
                
                # Mode 3每张图都重新检查文件序号
                file_counter = self._get_max_file_num()
                
                # 获取详情页
                detail_soup = self.web.get_soup(img_url, retries=50)
                if not detail_soup:
                    continue
                
                pic_time, pic_date, pic_id, pic_url, pic_tags, pic_filename = self._extract_metadata(detail_soup)
                if not pic_filename:
                    continue
                
                # 记录最新时间
                self._add_downloadtag(tag, pic_time)
                
                new_filename = f"{file_counter}_{self.replace_tag}_{pic_date}_{pic_id}.{pic_filename.split('.')[-1]}"
                original_save_path = os.path.join(config['path']['Gelbooru'], self.replace_tag, pic_filename)
                new_save_path = os.path.join(config['path']['new'], new_filename)
                
                # 检查是否已存在
                if os.path.exists(original_save_path):
                    skip_count += 1
                    if skip_count >= DOWN_SKIP_THRESHOLD:
                        # 连续skip达到阈值，停止下载这个tag
                        #self.log(f'{tag}({offset}) skip threshold reached, stopping')
                        break
                else:
                    skip_count = 0  # 重置skip计数
                    
                    # 下载图片
                    image_content = self.web.download_image(pic_url, retries=50)
                    
                    if not image_content:
                        self.failed_cnt += 1
                        self.log(f'failed {tag} {pic_filename}')
                        self._write_failed(tag, pic_url, pic_time, pic_id, pic_filename, pic_tags)
                    else:
                        size = len(image_content)
                        self.total_download_size += size
                        total_size_this_tag += size
                        
                        # 保存图片
                        image_metadata = {
                            'pic_id': pic_id,
                            'tag_name': self.replace_tag,
                            'filename': pic_filename,
                            'new_filename': new_filename,
                            'file_path': original_save_path,
                            'file_size': size,
                            'pic_url': pic_url,
                            'pic_tags': pic_tags,
                            'pic_time': pic_time,
                            'pic_date': pic_date,
                            'save_path': original_save_path,
                            'new_path': new_save_path
                        }
                        
                        self._save_image(image_metadata, image_content)
                        self.downed_cnt += 1
                        downloaded_this_tag += 1
                        file_counter += 1
                        
                        formatted_size = format_size(size)
                        self.log(f'{tag}({offset}) {file_counter-1} {pic_time} {pic_filename} {formatted_size}')
                        
                        # 直接写入tag自己的tags.txt（Mode 3每个tag也是独立的）
                        set_tag.update_tags(self.replace_tag, f"{tag}|{pic_time}|{pic_filename}|{pic_id}|{pic_tags}")
                
                # 每42张输出一次进度
                if i == 42:
                    self.log(f'page: {page} count: 42')
            
            # 如果skip达到阈值，停止翻页
            if skip_count >= DOWN_SKIP_THRESHOLD:
                break
        
        # 整理tags.txt - 直接在线程中执行
        set_tag.update_tags(self.replace_tag, None, 'u')
        
        # 输出这个tag的统计
        if downloaded_this_tag > 0:
            size_str = format_size(total_size_this_tag)
            self.log(f'count: {downloaded_this_tag} size: {size_str}')
            
            # 同步执行缩略图任务
            _run_sample_sync(self.replace_tag)
        
        # 收集统计信息，确保主流程能正确统计
        self.result['statistics'] = {
            'downloaded': self.downed_cnt,
            'failed': self.failed_cnt,
            'total_size': self.total_download_size
        }
        return downloaded_this_tag > 0
    
    def _check_expire(self):
        """检查过期标签数量（收集数据不写入）"""
        nulltag = 0
        nulltag_path = config['path'].get('nulltag', config['path'].get('deadtag'))
        if nulltag_path and os.path.exists(nulltag_path):
            with open(nulltag_path, 'r') as fd:
                for line in fd:
                    nulltag += 1
            self.log(f'expired tag: {nulltag}')


def down_single(tag, tagjs):
    """单标签下载入口（供外部调用）"""
    downloader = Downloader()
    return downloader.download_single(tag, tagjs)


def down_batch_mode3_queue(task_queue, offset=0):
    """
    Mode 3 队列版本（从任务队列中取tag下载）
    
    特点:
    - 动态任务分配：工作线程从共享队列取tag
    - 自动负载均衡：完成快的线程处理更多任务
    - 线程安全：queue.Queue 保证并发安全
    - 每个线程有独立的 .start 文件，删除对应文件只停止对应线程
    
    Args:
        task_queue: queue.Queue 实例，包含待处理的tag
        offset: 线程编号
    
    Returns:
        dict: 收集的所有数据
    """
    downloader = Downloader()
    downloader.init_batch(offset)
    downloader.log('Start')
    
    # 创建启动文件
    with open(downloader.startfile, 'w') as f:
        f.write('')
    
    done_tags = []
    processed_count = 0
    interrupted = False  # 标记是否被中断
    
    while True:
        # 先检查自己的启动文件（在取任务前检查，快速响应中断）
        if not downloader._chk_start():
            downloader.log(f'Start file deleted, exiting...')
            interrupted = True
            break
        
        try:
            # 从队列取tag（超时0.5秒，更快响应中断）
            tag = task_queue.get(timeout=0.5)
            
            # 遇到结束标记（None），退出循环
            if tag is None:
                task_queue.task_done()
                #downloader.log('Received stop signal')
                break
            
            try:
                processed_count += 1
                
                # 检查启动文件（支持中断）- 在处理任务时再次检查
                if not downloader._chk_start():
                    downloader.log(f'Interrupted at tag: {tag}')
                    task_queue.task_done()
                    interrupted = True
                    break
                
                # 检查是否应该跳过
                if downloader._should_skip_tag(tag):
                    downloader.log(f'Skip tag {tag}')
                    continue
                
                # 获取当前页数用于日志
                file_counter = downloader._get_max_file_num()
                downloader.log(f'Tag({offset})/({processed_count}): {file_counter} {tag}')
                
                # 下载这个tag的新图片（使用skip逻辑）
                result = downloader._download_tag_batch(tag, offset, processed_count)
                
                # 不管是否有新下载，只要处理过就加入done_tags
                # 这样才能在tags.txt中把处理过的tag移到最后
                done_tags.append(tag)
            finally:
                # 确保任务标记为完成 (#16修复死锁风险)
                task_queue.task_done()
            
        except queue.Empty:
            # 队列为空且超时，检查是否真的完成了
            if task_queue.empty():
                #downloader.log('Queue empty, exiting')
                break
            continue
            
        except Exception as e:
            downloader.log(f'Error processing tag: {str(e)}')
            try:
                task_queue.task_done()
            except Exception:
               pass
            continue
    
    # 删除启动文件
    if downloader._chk_start():
        downloader.result['remove_startfile'] = downloader.startfile
    
    # 输出统计信息（与旧代码格式一致）
    if downloader.downed_cnt > 0:
        total_size = downloader.total_download_size
        if total_size < 1024 * 1024:
            total_size_str = f"{total_size / 1024:.2f} Kb"
            avg_size_str = f"{total_size / downloader.downed_cnt / 1024:.2f} Kb"
        elif total_size < 1024 * 1024 * 1024:
            total_size_str = f"{total_size / 1024 / 1024:.2f} Mb"
            avg_size_str = f"{total_size / downloader.downed_cnt / 1024 / 1024:.2f} Mb"
        else:
            total_size_str = f"{total_size / 1024 / 1024 / 1024:.2f} Gb"
            avg_size_str = f"{total_size / downloader.downed_cnt / 1024 / 1024:.2f} Mb"
        downloader.log(f'Total size({offset}): {total_size_str}  avg size: {avg_size_str}')
        downloader.log(f'Total download({offset}): {downloader.downed_cnt} failed: {downloader.failed_cnt}')
    
    # 检查过期标签
    downloader._check_expire()
    
    downloader.log(f'End({offset}) tags:{processed_count} done:{len(done_tags)}')
    downloader.log('End')
    
    # 收集完成的tag列表和中断标志
    downloader.result['done_tags'] = done_tags
    downloader.result['interrupted'] = interrupted
    
    return downloader.result


def _batch_queue_worker(task_queue, offset, mode_name, tag_processor, stat_keys):
    """
    通用批量队列工作函数（Mode 6/7 公共逻辑）
    
    Args:
        task_queue: 任务队列
        offset: 线程编号
        mode_name: 模式名称（用于日志）
        tag_processor: 处理单个tag的函数 (downloader, tag) -> tuple of stats
        stat_keys: 统计键名列表，如 ['added', 'updated', 'skipped']
    
    Returns:
        dict: 统计结果
    """
    downloader = Downloader()
    downloader.init_batch(offset)
    downloader.log(f'{mode_name} Start')
    
    # 创建启动文件
    with open(downloader.startfile, 'w') as f:
        f.write('')
    
    processed_count = 0
    stats = {key: 0 for key in stat_keys}
    interrupted = False
    
    while True:
        # 检查启动文件
        if not downloader._chk_start():
            downloader.log('Start file deleted, exiting...')
            interrupted = True
            break
        
        try:
            tag = task_queue.get(timeout=0.5)
            
            if tag is None:
                task_queue.task_done()
                break
            
            try:
                processed_count += 1
                
                if not downloader._chk_start():
                    downloader.log(f'Interrupted at tag: {tag}')
                    task_queue.task_done()
                    interrupted = True
                    break
                
                if downloader._should_skip_tag(tag):
                    downloader.log(f'Skip tag {tag}')
                    continue
                
                downloader.log(f'{mode_name}({offset})/({processed_count}): {tag}')
                
                # 处理tag并收集统计
                result_stats = tag_processor(downloader, tag)
                for i, key in enumerate(stat_keys):
                    if i < len(result_stats):
                        stats[key] += result_stats[i]
                
                # 只在有变化时输出日志
                if any(result_stats):
                    stat_str = ', '.join(f'{k}={v}' for k, v in zip(stat_keys, result_stats))
                    downloader.log(f'Tag {tag}: {stat_str}')
                
            finally:
                task_queue.task_done()
            
        except queue.Empty:
            if task_queue.empty():
                break
            continue
            
        except Exception as e:
            downloader.log(f'Error processing tag: {str(e)}')
            try:
                task_queue.task_done()
            except Exception:
                pass
            continue
    
    # 删除启动文件
    if downloader._chk_start():
        downloader.result['remove_startfile'] = downloader.startfile
    
    stat_str = ', '.join(f'{k}={v}' for k, v in stats.items())
    downloader.log(f'Total: {stat_str}')
    downloader.log(f'End({offset}) processed:{processed_count}')
    downloader.log('End')
    
    downloader.result['statistics'] = stats
    downloader.result['processed_count'] = processed_count
    downloader.result['interrupted'] = interrupted
    
    return downloader.result


def update_batch_mode6_queue(task_queue, offset):
    """Mode 6: 更新图片信息（不下载，只对比并更新DB）"""
    return _batch_queue_worker(
        task_queue, offset, 
        mode_name="Update",
        tag_processor=_update_tag_info,
        stat_keys=['added', 'updated', 'skipped']
    )


def update_batch_mode7_queue(task_queue, offset):
    """Mode 7: 从本地 tags.txt 读取信息写入数据库（不联网）"""
    return _batch_queue_worker(
        task_queue, offset,
        mode_name="Import",
        tag_processor=_import_from_tags_txt,
        stat_keys=['added', 'skipped', 'not_found']
    )


def _import_from_tags_txt(downloader, tag):
    """
    从 tags.txt 导入图片信息到数据库
    
    Args:
        downloader: Downloader实例
        tag: 标签名
    
    Returns:
        tuple: (added_count, skipped_count, not_found_count)
    """
    replace_tag = downloader._normalize_tag(tag)
    added_count = 0
    skipped_count = 0
    not_found_count = 0
    
    # 检查本地文件夹是否存在
    local_path = os.path.join(downloader.gelbooru_path, replace_tag)
    if not os.path.exists(local_path):
        return 0, 0, 0
    
    # 读取 tags.txt
    tags_txt_path = os.path.join(local_path, 'tags.txt')
    if not os.path.exists(tags_txt_path):
        return 0, 0, 0
    
    # 获取DB中已有的记录
    db_records = downloader.db.get_local_filenames_by_tag(replace_tag)
    
    try:
        with open(tags_txt_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        downloader.log(f'Failed to read tags.txt: {e}')
        return 0, 0, 0
    
    for line in lines:
        line = line.strip()
        if not line or '|' not in line:
            continue
        
        parts = line.split('|')
        if len(parts) < 4:
            continue
        
        # 格式: tag_name|pic_time|filename|pic_id|pic_tags
        # 或者: tag_name|pic_time|filename|pic_id (旧格式，无pic_tags)
        original_tag = parts[0]
        pic_time = parts[1]
        pic_filename = parts[2]
        pic_id = parts[3]
        pic_tags = parts[4] if len(parts) > 4 else ''
        
        # 验证 pic_id 是数字
        if not pic_id.isdigit():
            continue
        
        # 检查本地文件是否存在
        file_path = os.path.join(local_path, pic_filename)
        if not os.path.exists(file_path):
            not_found_count += 1
            continue
        
        # 检查DB是否已有记录
        if pic_filename in db_records:
            skipped_count += 1
            continue
        
        # 计算文件大小
        try:
            file_size = os.path.getsize(file_path)
        except:
            file_size = 0
        
        # 提取日期
        pic_date = pic_time[:10] if len(pic_time) >= 10 else None
        
        # 构造 new_filename: 0_tag_name_pic_date_pic_id.ext
        ext = os.path.splitext(pic_filename)[1]
        new_filename = f"0_{replace_tag}_{pic_date}_{pic_id}{ext}"
        
        pic_data = {
            'pic_id': pic_id,
            'tag_name': replace_tag,
            'filename': pic_filename,
            'new_filename': new_filename,
            'file_path': file_path,
            'file_size': file_size,
            'pic_url': '',  # 从 tags.txt 导入没有 pic_url
            'pic_tags': pic_tags,
            'pic_time': pic_time,
            'pic_date': pic_date
        }
        
        downloader.db.add_picture(pic_data)
        added_count += 1
    
    return added_count, skipped_count, not_found_count


def _update_tag_info(downloader, tag):
    """
    更新单个tag的图片信息
    
    Args:
        downloader: Downloader实例
        tag: 标签名
    
    Returns:
        tuple: (added_count, updated_count, skipped_count)
    """
    replace_tag = downloader._normalize_tag(tag)
    added_count = 0
    updated_count = 0
    skipped_count = 0
    
    # 获取本地文件夹中的所有文件
    local_path = os.path.join(downloader.gelbooru_path, replace_tag)
    if not os.path.exists(local_path):
        downloader.log(f'Local folder not found: {local_path}')
        return 0, 0, 0
    
    # 获取本地所有图片文件名
    local_files = set()
    valid_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.webm'}
    for filename in os.listdir(local_path):
        ext = os.path.splitext(filename)[1].lower()
        if ext in valid_extensions:
            local_files.add(filename)
    
    if not local_files:
        downloader.log(f'No image files in {local_path}')
        return 0, 0, 0
    
    # 获取DB中已有的记录
    db_records = downloader.db.get_local_filenames_by_tag(replace_tag)
    
    # 遍历网站上的图片
    page = 0
    base_tag_url = downloader.base_url + tag + downloader._exclude_url(tag)
    
    while True:
        page += 1
        url = base_tag_url + downloader._get_page_url(page)
        soup = downloader.web.get_soup(url, retries=50)
        
        if not soup:
            break
        
        image_urls = downloader.web.get_image_list(soup)
        if not image_urls:
            break
        
        # 遍历图片
        for img_url in image_urls:
            # 获取详情页
            detail_soup = downloader.web.get_soup(img_url, retries=50)
            if not detail_soup:
                continue
            
            pic_time, pic_date, pic_id, pic_url, pic_tags, pic_filename = downloader._extract_metadata(detail_soup)
            if not pic_filename:
                continue
            
            # 检查本地是否存在该文件
            if pic_filename not in local_files:
                skipped_count += 1
                continue
            
            # 检查DB是否已有记录
            if pic_filename in db_records:
                # 已有记录，检查是否需要更新（例如 pic_url 为空）
                existing = db_records[pic_filename]
                update_data = {}
                
                # 检查哪些字段需要更新
                if not existing.get('pic_url') and pic_url:
                    update_data['pic_url'] = pic_url
                if not existing.get('pic_tags') and pic_tags:
                    update_data['pic_tags'] = pic_tags
                if not existing.get('pic_time') and pic_time:
                    update_data['pic_time'] = pic_time
                    update_data['pic_date'] = pic_date
                
                if update_data:
                    if downloader.db.update_picture(existing['pic_id'], replace_tag, update_data):
                        updated_count += 1
                        downloader.log(f'Tag {tag} db update: {pic_filename} (ID: {pic_id})')
                continue
            
            # 本地有文件但DB无记录，添加记录
            file_path = os.path.join(local_path, pic_filename)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            pic_data = {
                'pic_id': pic_id,
                'tag_name': replace_tag,
                'filename': pic_filename,
                'new_filename': None,
                'file_path': file_path,
                'file_size': file_size,
                'pic_url': pic_url,
                'pic_tags': pic_tags,
                'pic_time': pic_time,
                'pic_date': pic_date
            }
            
            downloader.db.add_picture(pic_data)
            added_count += 1
            downloader.log(f'Tag {tag} db insert: {pic_filename} (ID: {pic_id})')
        
        # 每页处理完后检查是否中断
        if not downloader._chk_start():
            break
    
    return added_count, updated_count, skipped_count
