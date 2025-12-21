import write_file
import os
from config import config


def init_input(ind = 0):
    #从input文件中读取tag信息，并放到tagjson文件中，并更新tags.txt放到最后
    tagjson = {}
    tagjson = write_file.readjs(config['path']['tagjson'])
    input = write_file.readfile(config['path']['input'])
    if ind == 0:
        for i in input:
            if i[:5] == 'done ':
                delete_tagjson(i.strip().split(' ')[1])
            if i[:3] != 'TAG' and i[:5] != 'done ':
                if i.strip().split(' ') != [''] and i.strip().split(' ')[0] not in tagjson:
                    list_tag = i.strip().split(' ')
                    if len(list_tag) == 1:
                        list_tag = list_tag + [1, 0, '0']
                        set_input(tagjson, list_tag)
                    elif len(list_tag) == 2:
                        list_tag = list_tag + [0, '0']
                        set_input(tagjson, list_tag)
                    elif len(list_tag) == 3:
                        list_tag = list_tag + [0, '0']
                        set_input(tagjson, list_tag)
                    elif len(list_tag) == 4:
                        set_input(tagjson, list_tag)
    if len(tagjson) == 0:
        set_input(tagjson, [' ', 1, 1, 0, '0'], 9)
    write_file.writejs(config['path']['tagjson'], tagjson)
    add_list_tags([key for key in tagjson.keys() if tagjson[key]['status'] not in [7, 8, 9]], 'a')

def add_dead_tag():
    tags = write_file.readfile(config['path']['tags'])
    expire_tag = write_file.readfile(config['path']['nulltag'])
    tag_list1 = [item.rstrip('\n') for item in tags]
    tag_list2 = [item.rstrip('\n') for item in expire_tag]
    tags = tag_list2 + tag_list1
    #print(tags)
    with open(config['path']['tags'], 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, tags)) + '\n')
    with open(config['path']['nulltag'], 'w', encoding='utf-8') as f:
        pass

def set_input(tag, list_tag, status = 0):
    if status != 8:
        tag[list_tag[0]] = {
            "startpage": 1,
            "endpage": int(list_tag[1]),
            "start_pic": int(list_tag[2]),
            "end_pic": str(list_tag[3]),
            "status": status
        }
    else:
        if tag[list_tag[0]]['status'] == 8:
            tag[list_tag[0]] = {
                "tag": list_tag[1],
                "start_pic": list_tag[2],
                "status": status
            }

def read_tags():
    tags = write_file.readfile(config['path']['tags'])
    set_tag = set()
    list_tag = []
    for i in tags:
        if i.strip() not in set_tag and i.strip() != '':
            set_tag.add(i.strip())
            list_tag.append(i.strip())
    return list_tag


def add_list_tags(tags, mode = 'w'):
    with open(config['path']['tags'], mode, encoding='utf-8') as f:
        f.write('\n'.join(map(str, tags)) + '\n')


def save_list_tags(list_tag):
    tags = read_tags()
    for i in list_tag:
        while i in tags:
            tags.remove(i)
        tags.append(i)
    with open(config['path']['tags'], 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, list_tag)) + '\n')


def update_tagjson(tag, tag_json):
    tagjson = write_file.readjs(config['path']['tagjson'])
    if tag in tagjson:
        tagjson[tag] = tag_json
        write_file.writejs(config['path']['tagjson'], tagjson)


def set_input_done(tag):
    input = write_file.readfile(config['path']['input'])
    f = open(config['path']['input'], 'w', encoding='utf-8')
    for i in input:
        if i.strip().split(' ')[0] == tag:
            f.write('done ' + i)
        else:
            if i.strip().split(' ') != ['']:
                f.write(i)
    f.close()
    delete_tagjson(tag)

def del_input_done():
    input = write_file.readfile(config['path']['input'])
    f = open(config['path']['input'], 'w', encoding='utf-8')
    for i in input:
        if i[:4] != 'done':
            if i.strip():
                f.write(i)
    f.close()

def delete_tagjson(tag):
    tagjson = write_file.readjs(config['path']['tagjson'])
    if tag in tagjson:
        del tagjson[tag]
        write_file.writejs(config['path']['tagjson'], tagjson)

def read_list_tag(ind = 0):
    #读取tags.txt第一个tag
    tags = read_tags()
    offset = 0 + ind
    return tags[offset]

def add_tags(done_list):
    #添加tag到tags.txt最后
    tags = read_tags()
    for tag in done_list:
        while tag in tags:
            tags.remove(tag)
        tags.append(tag)
    add_list_tags(tags)

def read_tagjson():
    #读取tagjson
    return write_file.readjs(config['path']['tagjson'])


def add_folder_tag():
    #根据folder目录更新tag
    folder_path = config['path']['Gelbooru']
    list_file = config['path']['tags']
    exclude_file = config['path']['exauthor']
    dead_tag = config['path']['deadtag']

    exclude = set()
    try:
        with open(exclude_file, 'r') as f:
            exclude = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        print(f"[Warning] {exclude_file} not found, skip excluding")
    try:
        with open(dead_tag, 'r') as f:
            exclude = exclude.union({line.strip() for line in f if line.strip()})
    except FileNotFoundError:
        print(f"[Warning] {dead_tag} not found, skip excluding")

    existing = set()
    try:
        with open(list_file, 'r') as f:
            existing = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        pass

    new_folders = []
    with os.scandir(folder_path) as it:
        for entry in it:
            if (entry.is_dir() and entry.name not in existing
                    and entry.name not in exclude):
                new_folders.append(entry.name)

    if new_folders:
        with open(list_file, 'a', buffering=1) as f:
            for folder in new_folders:
                f.write(folder + '\n')

def update_tags(tag, line, mode = 'a'):
    #tags.txt更新
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
    tagpath = os.path.join(config['path']['Gelbooru'], tag, 'tags.txt')
    if not os.path.exists(tagpath):
        f = open(tagpath,'w')
        f.close()
    if mode == 'a':
        with open(tagpath, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    else:
        with open(tagpath, 'r') as f:
            existing = [l.strip().replace(' - Image View -  |', '') for l in f.readlines()]
        all_lines = existing
        seen = set()
        unique_lines = []
        for l in reversed(all_lines):
            if l not in seen and '|' in l and is_number(l.split('|')[3]):
                seen.add(l)
                unique_lines.insert(0, l)
        current_tag = tag
        modified = []
        for l in unique_lines:
            parts = l.split('|')
            parts[0] = current_tag
            modified.append('|'.join(parts))
        modified.sort(key=lambda x: int(x.split('|')[3]), reverse=True)
        with open(tagpath, 'w') as f:
            f.write('\n'.join(modified) + '\n')

def add_expire_tag(expire_tag):
    #把过期tag添加到最后
    tags = write_file.readfile(config['path']['nulltag'])
    tag_list = [item.rstrip('\n') for item in tags]
    while expire_tag in tag_list:
        tag_list.remove(expire_tag)
    tag_list.append(expire_tag)
    with open(config['path']['nulltag'], 'w', encoding='utf-8') as f:
        f.write('\n'.join(map(str, tag_list)) + '\n')

def main():
    #add_folder_tag()
    #init_input()
    add_dead_tag()
    #add_tags(['ukiko_ruu','232323','33333333'])
    #add_down_all_tag()
    #add_tags('ukiko_ruu')
    # a = read_list_tag(0)
    # add_tags(a)
    #add_expire_tag('mr_zhuo')


main()