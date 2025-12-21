from config import config
import os
import re
import time
import datetime
import random
import requests
import bs4
import argparse
import json
from win32file import CreateFile, SetFileTime, GetFileTime, CloseHandle
from win32file import GENERIC_READ, GENERIC_WRITE, OPEN_EXISTING
from pywintypes import Time
from bs4 import BeautifulSoup
from tqdm import tqdm
import set_tag
import sampletag
import threading
file_lock = threading.Lock()


class Download(object):

    def __init__(self):
        self.config = config
        self.extag = self.readfile(self.config['path']['extag'], 'r')
        self.exauthor = self.readfile(self.config['path']['exauthor'], 'r')
        self.expic = set(self.readfile(self.config['path']['expic'], 'r'))
        self.dead_tag = self.readfile(self.config['path']['deadtag'], 'r')
        self.expir_tag = self.readfile(self.config['path']['nulltag'], 'r')
        self.downed = 0
        self.failed = 0
        self.totaldownsize = 0
        self.tag_time = {}

    def init_downall(self, offset = 0):
        self.logpath = self.config['path']['taglog'] + str(offset) + '.txt'
        self.startfile = self.config['path']['startfile'] + str(offset) + '.start'
        self.down_mode = 1

    def init_downtag(self, offset = 0):
        self.logpath = self.config['path']['new'] + '\\z ' + self.retag + '.txt'
        self.startfile = self.config['path']['new'] + '\\zzz' + self.retag + '.start'
        self.down_mode = 0

    def OffsetAndStruct(self, times, format, offset):
        return time.localtime(
            time.mktime(time.strptime(times, format)) + offset)

    def create_folder(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
            self.log(f'{self.retag} created')

    def replace_tag(self):
        self.retag = self.tag.replace(":", "_").replace(".", "_").replace("+", "_")

    def log(self, line, end='\n', timestamp=True):
        line1 = line
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        line = current_time + ' | ' + line
        print(line, end=end)
        if timestamp:
            line = current_time + ' | ' + line1
        else:
            line = line1
        f = open(self.logpath, 'a', encoding='utf-8')
        f.write(line + end)
        f.close()

    def chk_start(self):
        return os.path.exists(self.startfile)

    def add_downloadtag(self, tag, tag_time):
        if tag not in self.tag_time:
            self.tag_time[tag] = tag_time
        else:
            self.tag_time[tag] = max(self.tag_time[tag], tag_time)

    def wait_to_process(self, done_list, list_tag_time, mod = 0):
        if mod == 1:
            set_tag.add_tags(done_list)
        self.write_tag_time(list_tag_time)


    def write_tag_time(self, list_tag_time):
        from operator import itemgetter
        file_path = self.config['path']['downtag']
        entries = set()

        def merge_dicts(dict1, dict2):

            def parse_time(time_str):
                return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

            merged = {}

            for key, value in dict1.items():
                time_value = parse_time(value)
                merged[key] = time_value
            for key, value in dict2.items():
                time_value = parse_time(value)
                if key in merged:
                    merged[key] = max(merged[key], time_value)
                else:
                    merged[key] = time_value
            return {key: dt.strftime("%Y-%m-%d %H:%M:%S") for key, dt in merged.items()}

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # 检查'time:'是否存在
                    if 'time:' not in line:
                        continue
                    parts = line.split('time:')
                    if len(parts) != 2:
                        continue
                    name_part, time_part = parts
                    # 检查': '是否存在
                    if ': ' not in name_part:
                        continue
                    tag_id_name = name_part.split(': ', 1)
                    if len(tag_id_name) != 2:
                        continue
                    tag_id, name = tag_id_name
                    entries.add(name.strip() + '|' + time_part.strip())

        dict_entries = {}
        for entry in entries:
            name, time_str = entry.split('|', 1)
            dict_entries[name] = max(dict_entries.get(name, ''), time_str)

        dict_entries = merge_dicts(dict_entries, list_tag_time)

        entries = [(k, datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S'), v)
                for k, v in dict_entries.items()]
        entries.sort(key=itemgetter(1), reverse=False)

        content = []
        for idx, (name, _, time_str) in enumerate(entries, 1):
            entry_id = len(entries) - idx + 1
            line = f"tag {entry_id:4}: {name.ljust(50)} time: {time_str}\n"
            content.append(line)

        with open(file_path, 'w') as f:
            f.writelines(content)

    def log_end(self):
        print('')
        f = open(self.logpath, 'a', encoding='utf-8')
        f.write('\n')
        f.close()

    def getHTMLText(self, url, max_retries=10):
        cnt = 0
        while cnt < max_retries:
            try:
                r = requests.get(url=url, 
                                 headers=self.config['headers'],
                                 timeout=10)
                r.encoding = r.apparent_encoding
                soup = BeautifulSoup(r.text, "html.parser")
                return soup
            except Exception as e:
                print(f'{str(e)}')
                time.sleep(1)
                cnt += 1
        return None

    def down_pic(self, url, n):
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry_strategy = Retry(total=n,
                               status_forcelist=[429, 500, 502, 503, 504],
                               allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry_strategy)

        try:
            with requests.Session() as session:
                session.mount("https://", adapter)
                response = session.get(url,
                                       headers=self.config['headers'],
                                       timeout=(5, 8),
                                       stream=True)
                response.raise_for_status()

                content = b''
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        content += chunk
                return content

        except requests.exceptions.RequestException as e:
            print(f"下载失败: {e} (URL: {url})")
        except Exception as e:
            print(f"未知错误: {e}")
        finally:
            if 'response' in locals():
                response.close()
        return None

    def geturlend(self, page):
        page_num = 42
        if page == 1:
            page_url = ''
        else:
            page_url = "&pid=" + str(page_num * (page - 1))
        return page_url

    def get_page(self, url, tag, n=10):
        for attempt in range(n):
            try:
                soup = self.getHTMLText(url)
                if soup:
                    return soup
                else:
                    raise ValueError("getHTMLText返回空对象")
            except Exception as e:
                self.log(f"get image failed [{attempt+1}]")
                print(f"{str(e)}")
                time.sleep(2 ** attempt)
        print(f"警告: 标签[{tag}]下载失败，已达最大重试次数({n})")
        return None

    def readfile(self, openpath, openmode):
        with open(openpath, openmode) as f:
            return [line.rstrip() for line in f]

    def exclude_url(self, tag):
        url = ''
        for i in self.config['tag']['default_exclude_tags']:
            url = url + '+-' + i
        if tag in self.extag:
            for i in self.config['tag']['excludetag']:
                url = url + '+-' + i
        for i in self.exauthor:
            a = i.rstrip("\n").split(sep=',')
            if len(a) > 1:
                if tag == a[0]:
                    b = a
                    del b[0]
                    for j in b:
                        url = url + '+' + j
        return url

    def write_failed(self, *args):
        with open(self.config['path']['failed'], 'a', encoding='utf-8') as f:
            for arg in args:
                f.write(str(arg) + '|')
            f.write('\n')

    def get_tag_ind(self, ind):
        self.cnt = 0
        maxcnt = 0
        pattern = re.compile(r'(\d{1,4})_')
        target_dir = self.config['path']['new']
        with os.scandir(target_dir) as entries:
            for entry in entries:
                if entry.is_file():
                    filename = entry.name
                    match = pattern.search(filename)
                    if match:
                        current_num = int(match.group(1))
                        if current_num > maxcnt:
                            maxcnt = current_num
        self.cnt = maxcnt + 1

    def down_tag(self, down_mode=0, ind = 0):
        self.end = False
        self.down_mode = down_mode
        format1 = "%Y-%m-%d %H:%M:%S"
        start_time = time.time()
        if down_mode == 0:
            #单个tag下载
            self.get_tag_ind(ind)
            retry = 100
            self.add_downloadtag(self.tag, '2000-01-01 00:00:00')
            self.tagjs['status'] = 1
            sleep_time = random.uniform(0, 4)
            time.sleep(sleep_time)
            set_tag.update_tagjson(self.tag, self.tagjs)
            if self.tag in self.exauthor:
                return
            with open(self.startfile, 'w', encoding='utf-8') as f:
                f.write('')
            path = os.path.join(self.config['path']['Gelbooru'], self.retag)
            self.create_folder(path)
            self.log(f"download page:({self.tagjs['startpage']}/{self.tagjs['endpage']}/{self.tagjs['start_pic']}/{self.tagjs['end_pic']}) Page url:{self.config['url']+self.tag}")
            for page in range(self.tagjs['startpage'], self.tagjs['endpage']+1):
                if not self.chk_start():
                    self.tagjs['status'] = 2
                    sleep_time = random.uniform(0, 4)
                    time.sleep(sleep_time)
                    set_tag.update_tagjson(self.tag, self.tagjs)
                    break
                url = self.config['url'] + self.tag + self.exclude_url(self.tag) + self.geturlend(page)
                soup = self.get_page(url, self.tag, 50)
                self.list_url = [x.find_all('a')[0]['href'] for x in soup.find_all('article')]
                if page == 1 and not self.list_url:
                    if self.tag not in self.dead_tag:
                        set_tag.add_expire_tag(self.tag)
                if not self.list_url:
                    self.end = True
                    break
                self.log(f'page start: {page}/{self.tagjs["endpage"]} {url}')
                self.list_id = [re.findall('&id=(\d+)', x.find_all('a')[0]['href'])[0] for x in soup.find_all('article')]
                get_img = len(self.list_url)
                self.log(f'Total get {get_img}')
                for i, (image_url, id) in enumerate(zip(self.list_url, self.list_id), start=1):
                    self.tagjs['startpage'] = page
                    if not self.chk_start():
                        self.tagjs['status'] = 2
                        sleep_time = random.uniform(0, 4)
                        time.sleep(sleep_time)
                        set_tag.update_tagjson(self.tag, self.tagjs)
                        break
                    if i < self.tagjs['start_pic']:
                        continue
                    self.tagjs['start_pic'] = i
                    if id in self.expic:
                        self.log(f'skip ----{id}----')
                        continue
                    if i in [5,10,15,20,25,30,35,40]:
                        self.get_tag_ind(ind)
                    soup1 = self.get_page(image_url, self.tag, 50)
                    self.pic_time = soup1.find_all(string=re.compile('Posted:.*'))[0][8:]
                    self.add_downloadtag(self.tag, self.pic_time)
                    self.pic_date = self.pic_time[:10]
                    self.pic_id = soup1.find_all(string=re.compile('Id:.*'))[0][4:]
                    self.pic_url = soup1.find('meta', {'property': 'og:image'})['content']
                    self.pic_tags = soup1.title.string.split('- Image View -')[0].strip()
                    self.pic_filename = soup1.find('meta', {'property': 'og:image'})['content'].split('/')[-1]
                    self.new_name = str(self.cnt) + '_' + self.retag + '_' + self.pic_date + '_' + self.pic_id + '.' + self.pic_filename.split('.')[1]
                    self.save_path = os.path.join(self.config['path']['Gelbooru'], self.retag, self.pic_filename)
                    self.new_path = os.path.join(self.config['path']['new'], self.new_name)
                    pic = None
                    if os.path.exists(self.save_path):
                        self.log(f'{i}/{get_img} skip {self.pic_filename} {self.pic_time}')
                        set_tag.update_tags(self.retag, self.tag + '|' + self.pic_time + '|' + self.pic_filename + '|' + self.pic_id + '|' + self.pic_tags)
                    else:
                        while retry > 0:
                            pic = self.down_pic(self.pic_url, 50)
                            retry -= 1
                            if pic:
                                break
                        retry = 100
                        if not pic:
                            self.failed += 1
                            self.log(f'failed {self.tag} {self.pic_filename}')
                            self.write_failed(self.tag, self.pic_url, self.pic_time, self.pic_id, self.pic_filename, self.pic_tags)
                        else:
                            size = len(pic)
                            self.totaldownsize += size
                            if round(size/ 1024 / 1024, 2) < 1:
                                currentsize = str(round(size / 1024,2)) + ' Kb'
                            elif round(size/ 1024 / 1024 / 1024, 2) < 1:
                                currentsize = str(round(size / 1024 / 1024,2)) + ' Mb'
                            else:
                                currentsize = str(round(size / 1024 / 1024 / 1024,2)) + ' Gb'
                            #save picture
                            with open(self.save_path, 'wb') as f:
                                f.write(pic)
                            cTime_t = self.OffsetAndStruct(self.pic_time, format1,0)
                            createTimes = Time(time.mktime(cTime_t))
                            fh = CreateFile(self.save_path, GENERIC_READ | GENERIC_WRITE,0, None, OPEN_EXISTING, 0, 0)
                            SetFileTime(fh, createTimes, createTimes, createTimes)
                            CloseHandle(fh)
                            #save new pic
                            with open(self.new_path, 'wb') as fp:
                                fp.write(pic)
                            self.downed += 1
                            pic = None
                            self.log(f'{i}/{get_img}({self.tagjs["startpage"]}/{self.tagjs["endpage"]}) {self.tag} {self.cnt} {self.pic_time} {self.pic_filename} {currentsize}')
                            set_tag.update_tags(self.retag, self.tag + '|' + self.pic_time + '|' + self.pic_filename + '|' + self.pic_id + '|' + self.pic_tags)
                    if self.tagjs['end_pic'] != '0' and self.pic_id == self.tagjs['end_pic']:
                        self.end = True
                        set_tag.set_input_done(self.tag)
                        if self.chk_start():
                            os.remove(self.startfile)
                if self.chk_start():
                    self.tagjs['start_pic'] = 0
            set_tag.update_tags(self.retag, None, 'u')
            self.end = True
            self.log(f'Total time: {str(round((time.time() - start_time) / 60))} mins')
            if self.downed > 0:
                if round(self.totaldownsize / 1024 / 1024, 2) < 1:
                    totalsize = str(round(self.totaldownsize / 1024,2)) + ' Kb'
                    avgsize = str(round(self.totaldownsize/self.downed / 1024, 2)) + ' Kb'
                elif round(self.totaldownsize / 1024 / 1024 / 1024, 2) < 1:
                    totalsize = str(round(self.totaldownsize / 1024 / 1024,2)) + ' Mb'
                    avgsize = str(round(self.totaldownsize/self.downed / 1024 / 1024, 2)) + ' Mb'
                else:
                    totalsize = str(round(self.totaldownsize / 1024 / 1024 / 1024,2)) + ' Gb'
                    avgsize = str(round(self.totaldownsize/self.downed / 1024 / 1024, 2)) + ' Mb'
                self.log(f'Total size: {totalsize}  avg size: {avgsize}')
                self.log(f'Total download: {self.downed} failed: {self.failed}')
                sampletag.main(self.retag)
            self.tagjs['status'] = 2
            sleep_time = random.uniform(0, 4)
            time.sleep(sleep_time)
            set_tag.update_tagjson(self.tag, self.tagjs)
            if self.end and self.chk_start():
                set_tag.set_input_done(self.tag)
                os.remove(self.startfile)
        elif down_mode == 1:
            #下载所有tag
            retry = 100
            self.add_downloadtag(self.tag, '2000-01-01 00:00:00')
            self.down_skip = 2
            self.end = False
            skip = 0
            page = 0
            cnts = 0
            totalsize = 0
            if self.tag in self.exauthor:
                return
            if self.chk_start():
                path = os.path.join(self.config['path']['Gelbooru'], self.retag)
                self.create_folder(path)
                while not self.end:
                    page += 1
                    url = self.config['url'] + self.tag + self.exclude_url(self.tag) + self.geturlend(page)
                    soup = self.get_page(url, self.tag, 50)
                    self.list_url = [x.find_all('a')[0]['href'] for x in soup.find_all('article')]
                    if page == 1 and not self.list_url:
                        if self.tag not in self.dead_tag:
                            set_tag.add_expire_tag(self.tag)
                        self.end = True
                        break
                    if not self.list_url:
                        self.end = True
                        break
                    self.list_id = [re.findall('&id=(\d+)', x.find_all('a')[0]['href'])[0] for x in soup.find_all('article')]
                    get_img = len(self.list_url)
                    for i, (image_url, id) in enumerate(zip(self.list_url, self.list_id), start=1):
                        if id in self.expic:
                            self.log(f'skip ----{id}----')
                            continue
                        self.get_tag_ind(ind)
                        soup1 = self.get_page(image_url, self.tag, 50)
                        self.pic_time = soup1.find_all(string=re.compile('Posted:.*'))[0][8:]
                        self.add_downloadtag(self.tag, self.pic_time)
                        self.pic_date = self.pic_time[:10]
                        self.pic_id = soup1.find_all(string=re.compile('Id:.*'))[0][4:]
                        self.pic_url = soup1.find('meta', {'property': 'og:image'})['content']
                        self.pic_tags = soup1.title.string.split('- Image View -')[0].strip()
                        self.pic_filename = soup1.find('meta', {'property': 'og:image'})['content'].split('/')[-1]
                        self.new_name = str(self.cnt) + '_' + self.retag + '_' + self.pic_date + '_' + self.pic_id + '.' + self.pic_filename.split('.')[1]
                        self.save_path = os.path.join(self.config['path']['Gelbooru'], self.retag, self.pic_filename)
                        self.new_path = os.path.join(self.config['path']['new'], self.new_name)
                        pic = None
                        if os.path.exists(self.save_path):
                            skip += 1
                            if skip >= self.down_skip:
                                self.end = True
                                break
                        else:
                            while retry > 0:
                                pic = self.down_pic(self.pic_url, 50)
                                retry -= 1
                                if pic:
                                    break
                            retry = 100
                            if not pic:
                                self.failed += 1
                                self.log(f'failed {self.tag} {self.pic_filename}')
                                self.write_failed(self.tag, self.pic_url, self.pic_time, self.pic_id, self.pic_filename, self.pic_tags)
                            else:
                                cnts += 1
                                size = len(pic)
                                self.totaldownsize += size
                                totalsize += size
                                if round(size/ 1024 / 1024, 2) < 1:
                                    currentsize = str(round(size / 1024,2)) + ' Kb'
                                elif round(size/ 1024 / 1024 / 1024, 2) < 1:
                                    currentsize = str(round(size / 1024 / 1024,2)) + ' Mb'
                                else:
                                    currentsize = str(round(size / 1024 / 1024 / 1024,2)) + ' Gb'
                                if round(totalsize/ 1024 / 1024, 2) < 1:
                                    currenttotal = str(round(totalsize / 1024,2)) + ' Kb'
                                elif round(totalsize/ 1024 / 1024 / 1024, 2) < 1:
                                    currenttotal = str(round(totalsize / 1024 / 1024,2)) + ' Mb'
                                else:
                                    currenttotal = str(round(totalsize / 1024 / 1024 / 1024,2)) + ' Gb'

                                #save picture
                                with open(self.save_path, 'wb') as f:
                                    f.write(pic)
                                cTime_t = self.OffsetAndStruct(self.pic_time, format1,0)
                                createTimes = Time(time.mktime(cTime_t))
                                fh = CreateFile(self.save_path, GENERIC_READ | GENERIC_WRITE,0, None, OPEN_EXISTING, 0, 0)
                                SetFileTime(fh, createTimes, createTimes, createTimes)
                                CloseHandle(fh)
                                #save new pic
                                with open(self.new_path, 'wb') as fp:
                                    fp.write(pic)
                                self.downed += 1
                                pic = None
                                self.log(f'{self.tag}({ind}) {self.cnt} {self.pic_time} {self.pic_filename} {currentsize}')
                                set_tag.update_tags(self.retag, self.tag + '|' + self.pic_time + '|' + self.pic_filename + '|' + self.pic_id + '|' + self.pic_tags)
                        if i == 42:
                            self.log(f'page: {page} count: 42')
                set_tag.update_tags(self.retag, None, 'u')
            if cnts != 0:
                self.log(f'count: {cnts} size: {currenttotal}')
        else:
            #下载failed tag
            pass
        list_tag_time = self.tag_time
        return list_tag_time


    def check_expire(self):
        nulltag = 0
        with open(self.config['path']['nulltag'], 'r') as fd:
            for line in fd:
                nulltag += 1
        self.log(f'expired tag: {nulltag}')

    def down_single(self, tag, tagjs = {}, ind = 0):
        self.tag = tag
        self.tagjs = tagjs
        self.replace_tag()
        self.init_downtag(ind)
        self.log('Start')
        list_tag_time = self.down_tag(0, ind)
        self.check_expire()
        return list_tag_time
    
    def down_all(self, tag, offset = 0, cnt = 0):
        self.tag = tag
        if cnt == 0:
            self.log(f'Tag({offset}): {self.cnt} {self.tag}')
        else:
            self.log(f'Tag({offset})/({cnt}): {self.cnt} {self.tag}')
        if self.chk_start():
            self.replace_tag()
            list_tag_time = self.down_tag(1, offset)
        return list_tag_time

def down_single(tag, tagjs, ind = 0):
    download = Download()
    list_tag_time = download.down_single(tag,tagjs, ind)
    #download.wait_to_process(None, list_tag_time, ind)
    download.log('End')
    download.log_end()
    return list_tag_time

def down_all_tag(taglist, offset = 0):
    cnt = 0
    download = Download()
    download.init_downall(offset)
    download.log('Start')
    list_tag_time = {}
    with open(download.startfile, 'w', encoding='utf-8') as f:
        f.write('')
    done_list = []
    for tag in taglist:
        cnt += 1
        download.get_tag_ind(offset)
        list_tag_time = list_tag_time | (download.down_all(tag, offset, cnt) or {})
        done_list.append(tag)
        if not download.chk_start():
            break
    if download.downed > 0:
        if round(download.totaldownsize / 1024 / 1024, 2) < 1:
            totalsize = str(round(download.totaldownsize / 1024,2)) + ' Kb'
            avgsize = str(round(download.totaldownsize/download.downed / 1024, 2)) + ' Kb'
        elif round(download.totaldownsize / 1024 / 1024 / 1024, 2) < 1:
            totalsize = str(round(download.totaldownsize / 1024 / 1024,2)) + ' Mb'
            avgsize = str(round(download.totaldownsize/download.downed / 1024 / 1024, 2)) + ' Mb'
        else:
            totalsize = str(round(download.totaldownsize / 1024 / 1024 / 1024,2)) + ' Gb'
            avgsize = str(round(download.totaldownsize/download.downed / 1024 / 1024, 2)) + ' Mb'
        download.log(f'Total size({offset}): {totalsize}  avg size: {avgsize}')
        download.log(f'Total download({offset}): {download.downed} failed: {download.failed}')
        sampletag.main(download.retag)
    #download.wait_to_process(done_list, list_tag_time, 1)
    if download.chk_start():
        os.remove(download.startfile)
    download.check_expire()
    download.log(f'End({offset}) tags:{cnt}')
    download.log(f'End')
    download.log_end()
    return done_list, list_tag_time

def split_list(lst, n=6):
    result = {str(i + 1): [] for i in range(n)}  # 创建字典，键为"1"到"n"
    for i, item in enumerate(lst):
        result[str((i % n) + 1)].append(item)  # 直接计算索引并添加到对应键的子列表
    return result

def down_all_tags(taglist, n):
    import time
    import glob
    import concurrent.futures
    end_files = glob.glob(os.path.join(config['path']['new'], "*.start"))
    count = len(end_files)
    if count >= 6 or n - count < 0:
        return
    cnt = n - count
    splited_list = split_list(taglist, cnt)
    tasks = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=cnt) as executor:
        futures = {executor.submit(down_all_tag, data, i): i for i, data in splited_list.items()}

    for future in concurrent.futures.as_completed(futures):
        tag = futures[future]
        try:
            donelist, result_list = future.result()
            if donelist and result_list:
                download = Download()
                download.init_downall(0)
                download.wait_to_process(donelist, result_list, 1)
        except Exception as e:
            download = Download()
            download.init_downall(0)
            download.log(f'Error processing tag {tag}: {str(e)}')