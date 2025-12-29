# 标签管理模块
import os
import json
from core import config, get_database, load_tag_mapping


# ==================== 文件读写工具 ====================

def readjs(filepath):
    """读取 JSON 文件"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {}


def writejs(filepath, data):
    """写入 JSON 文件"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'写入 JSON 失败 {filepath}: {e}')


def readfile(filepath):
    """读取文本文件，返回行列表"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return [line.rstrip('\n') for line in f]
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f'读取文件失败 {filepath}: {e}')
        return []


def writefile(filepath, lines, mode='w'):
    """写入文本文件"""
    try:
        with open(filepath, mode, encoding='utf-8') as f:
            if lines:
                f.write('\n'.join(lines) + '\n')
    except Exception as e:
        print(f'写入文件失败 {filepath}: {e}')


# ==================== 标签管理核心函数 ====================

# 数据库迁移标志（可通过环境变量控制）
USE_DATABASE = os.getenv('USE_TAG_DATABASE', 'true').lower() == 'true'


def read_tagjson():
    """
    读取 tagjson 配置（从数据库读取）
    
    Returns:
        dict: 标签配置字典
    """
    db = get_database()
    return db.get_all_tag_progress()


def update_tagjson(tag, tag_json):
    """
    更新单个标签的配置（只更新数据库）
    
    Args:
        tag: 标签名
        tag_json: 标签配置字典 {'startpage', 'endpage', 'start_pic', 'end_pic', 'status'}
    """
    db = get_database()
    db.update_tag_progress(
        tag,
        startpage=tag_json.get('startpage'),
        start_pic=tag_json.get('start_pic'),
        status=tag_json.get('status')
    )


def delete_tagjson(tag):
    """
    删除标签配置（只删除数据库记录）
    
    Args:
        tag: 标签名
    """
    db = get_database()
    db.delete_tag_progress(tag)


def update_tags(tag, line, mode='a'):
    """
    更新标签的下载记录文件
    
    Args:
        tag: 标签名（retag，已经过特殊字符替换）
        line: 记录行（格式：tag|time|filename|id|tags）或 None（表示整理模式）
        mode: 'a' 追加，'u' 整理去重
    """
    tagpath = os.path.join(config['path']['Gelbooru'], tag, 'tags.txt')
    
    # 确保文件存在
    os.makedirs(os.path.dirname(tagpath), exist_ok=True)
    
    if mode == 'a' and line:
        # 追加模式：直接写入
        try:
            with open(tagpath, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
        except Exception as e:
            print(f'写入标签记录失败 {tagpath}: {e}')
    
    elif mode == 'u':
        # 整理模式：去重、排序
        try:
            with open(tagpath, 'r', encoding='utf-8') as f:
                lines = [l.strip().replace(' - Image View -  |', '') for l in f if l.strip()]
        except FileNotFoundError:
            return  # 文件不存在，无需整理
        except Exception as e:
            print(f'读取标签记录失败 {tagpath}: {e}')
            return
        
        # 去重（保留最后出现的）
        seen = set()
        unique_lines = []
        
        for line in reversed(lines):
            if not line or '|' not in line:
                continue
            
            parts = line.split('|')
            if len(parts) < 4:
                continue
            
            # 检查 ID 是否为数字
            try:
                pic_id = int(parts[3])
            except ValueError:
                continue
            
            if line not in seen:
                seen.add(line)
                unique_lines.insert(0, line)
        
        # 按 ID 降序排序
        unique_lines.sort(key=lambda x: int(x.split('|')[3]), reverse=True)
        
        # 写回文件
        try:
            writefile(tagpath, unique_lines)
        except Exception as e:
            print(f'整理标签记录失败 {tagpath}: {e}')


def add_expire_tag(expire_tag):
    """
    添加过期/失效标签到 nulltag 列表
    
    功能：
    - 检查是否为 replace_tag，如果是则转换为 original_tag
    - 避免记录替换后的tag（如 artist_abc），只记录原始tag（如 artist:abc）
    - 排除已在 deadtag 中的标签（双重保险）
    
    Args:
        expire_tag: 过期的标签名（可能是 original_tag 或 replace_tag）
    """
    nulltag_path = config['path']['nulltag']
    deadtag_path = config['path']['deadtag']
    
    # 加载 tag 映射关系（replace_tag -> original_tag）
    tag_mapping = load_tag_mapping(reverse=True)
    
    # 如果是 replace_tag，转换为 original_tag
    final_tag = tag_mapping.get(expire_tag, expire_tag)
    
    # 读取 deadtag 列表（排除已标记为永久失效的tag）
    dead_tags = set(readfile(deadtag_path))
    
    # 如果 tag 已在 deadtag 中，不添加到 nulltag
    if final_tag in dead_tags:
        return
    
    # 读取现有 nulltag 列表
    tags = readfile(nulltag_path)
    
    # 去重并添加到末尾
    tag_list = [t for t in tags if t and t != final_tag]
    tag_list.append(final_tag)
    
    # 写回
    writefile(nulltag_path, tag_list)


def set_input_done(tag):
    """
    标记标签为已完成（在 input 文件中添加 done 前缀）
    
    功能：
    1. 在 input.txt 中添加 done 前缀
    2. 从数据库删除进度记录
    3. 删除 .start 文件（如果存在）
    4. 将 tag 添加到 tags.txt
    
    Args:
        tag: 标签名
    """
    import os
    import glob
    
    input_path = config['path']['input']
    new_path = config['path']['new']
    
    # 1. 读取所有行
    try:
        lines = readfile(input_path)
    except Exception:
        return
    
    # 2. 修改对应行
    modified_lines = []
    for line in lines:
        if not line:
            continue
        
        parts = line.split()
        if parts and parts[0] == tag:
            modified_lines.append(f'done {line}')
        else:
            modified_lines.append(line)
    
    # 3. 写回
    writefile(input_path, modified_lines)
    
    # 4. 从数据库删除
    delete_tagjson(tag)
    
    # 5. 删除 .start 文件（支持多种格式）
    replace_tag = tag.replace('/', '_').replace('\\', '_')
    start_patterns = [
        os.path.join(new_path, f'zzz{replace_tag}.start'),
        os.path.join(new_path, f'{tag}.start'),
        os.path.join(new_path, f'zzztag{replace_tag}.start')
    ]
    
    for start_file in start_patterns:
        if os.path.exists(start_file):
            try:
                os.remove(start_file)
                print(f"  ✓ 删除启动文件: {os.path.basename(start_file)}")
            except Exception as e:
                print(f"  ⚠️  删除启动文件失败: {e}")
    
    # 6. 将 tag 添加到 tags.txt（如果未存在）
    try:
        tags_list = read_tags()
        if tag not in tags_list:
            add_tags([tag])
            print(f"  ✓ 已将 {tag} 添加到 tags.txt")
    except Exception as e:
        print(f"  ⚠️  添加到 tags.txt 失败: {e}")


# ==================== 辅助函数（保持向后兼容）====================

def read_tags():
    """
    读取 tags.txt 并去重
    
    Returns:
        list: 标签列表
    """
    tags = readfile(config['path']['tags'])
    
    # 去重，保持顺序
    seen = set()
    unique_tags = []
    for tag in tags:
        tag = tag.strip()
        if tag and tag not in seen:
            seen.add(tag)
            unique_tags.append(tag)
    
    return unique_tags


def add_tags(done_list):
    """
    添加标签到 tags.txt 末尾（已存在的先移除再添加）
    
    Args:
        done_list: 标签列表
    """
    tags = read_tags()
    
    # 移除已存在的标签
    for tag in done_list:
        while tag in tags:
            tags.remove(tag)
    
    # 添加到末尾
    tags.extend(done_list)
    
    # 写回
    writefile(config['path']['tags'], tags)


def add_folder_tag():
    """
    扫描 Gelbooru 目录，将新文件夹添加为标签
    """
    folder_path = config['path']['Gelbooru']
    list_file = config['path']['tags']
    exclude_file = config['path']['exauthor']
    dead_tag_file = config['path']['deadtag']
    
    # 读取排除列表
    exclude = set()
    if os.path.exists(exclude_file):
        exclude.update(readfile(exclude_file))
    if os.path.exists(dead_tag_file):
        exclude.update(readfile(dead_tag_file))
    
    # 读取已存在的标签
    existing = set(readfile(list_file))
    # 扫描文件夹
    new_folders = []
    try:
        with os.scandir(folder_path) as entries:
            for entry in entries:
                if (entry.is_dir() and 
                    entry.name not in existing and 
                    entry.name not in exclude):
                    new_folders.append(entry.name)
    except Exception as e:
        print(f'扫描文件夹失败 {folder_path}: {e}')
        return
    
    # 追加新标签（去重，避免重复添加）
    if new_folders:
        try:
            # 读取现有标签，避免重复
            existing_tags = set(readfile(list_file))
            to_add = [folder for folder in new_folders if folder not in existing_tags]
            if to_add:
                with open(list_file, 'a', encoding='utf-8') as f:
                    for folder in to_add:
                        f.write(folder + '\n')
                print(f'添加了 {len(to_add)} 个新标签')
        except Exception as e:
            print(f'写入标签失败: {e}')


def init_input(ind=0):
    """
    从 input 文件读取标签信息并初始化到数据库
    
    功能：
    - 识别 done tag1 时：删除数据库记录 + 删除 .start 文件 + 添加到 tags.txt
    - 新tag：初始化到数据库
    
    Args:
        ind: 索引（保留参数，向后兼容）
    """
    import os
    
    db = get_database()
    tagjson = read_tagjson()
    input_lines = readfile(config['path']['input'])
    new_path = config['path']['new']
    
    if ind == 0:
        for line in input_lines:
            line = line.strip()
            if not line or line.startswith('TAG'):
                continue
            
            # 处理 done 标记
            if line.startswith('done '):
                tag_name = line.split()[1] if len(line.split()) > 1 else None
                if tag_name:
                    # 删除数据库记录
                    delete_tagjson(tag_name)
                    
                    # 删除 .start 文件
                    replace_tag = tag_name.replace('/', '_').replace('\\', '_')
                    start_patterns = [
                        os.path.join(new_path, f'zzz{replace_tag}.start'),
                        os.path.join(new_path, f'{tag_name}.start'),
                        os.path.join(new_path, f'zzztag{replace_tag}.start')]
                    
                    for start_file in start_patterns:
                        if os.path.exists(start_file):
                            try:
                                os.remove(start_file)
                            except Exception:
                                pass
                    
                    # 添加到 tags.txt（如果未存在）
                    try:
                        tags_list = read_tags()
                        if tag_name not in tags_list:
                            add_tags([tag_name])
                    except Exception:
                        pass
                
                continue
            
            # 解析标签配置
            parts = line.split()
            if not parts or parts[0] in tagjson:
                continue
            
            # 补全参数：tag [endpage] [start_pic] [end_pic]
            if len(parts) == 1:
                parts.extend(['1', '0', '0'])
            elif len(parts) == 2:
                parts.extend(['0', '0'])
            elif len(parts) == 3:
                parts.append('0')
            
            # 添加到数据库
            db.init_tag_progress(
                tag=parts[0],
                endpage=int(parts[1]),
                start_pic=int(parts[2]),
                end_pic=str(parts[3]),
                status=0
            )
    
    # 确保至少有一个条目（防止空数据库）
    if not read_tagjson():
        db.init_tag_progress(
            tag=' ',
            endpage=1,
            start_pic=0,
            end_pic='0',
            status=9
        )
    
    # 更新 tags.txt
    tagjson = read_tagjson()
    active_tags = [tag for tag, info in tagjson.items() 
                   if info.get('status', 0) not in [7, 8, 9]]
    if active_tags:
        try:
            with open(config['path']['tags'], 'a', encoding='utf-8') as f:
                f.write('\n'.join(active_tags) + '\n')
        except Exception as e:
            print(f'更新 tags.txt 失败: {e}')


def del_input_done():
    """删除 input 文件中的 done 标记行"""
    input_lines = readfile(config['path']['input'])
    active_lines = [line for line in input_lines 
                    if line and not line.startswith('done')]
    writefile(config['path']['input'], active_lines)


def add_dead_tag():
    """将过期标签合并到主标签列表"""
    tags = readfile(config['path']['tags'])
    expire_tags = readfile(config['path']['nulltag'])
    
    #合并（过期标签在前）
    all_tags = expire_tags + tags
    writefile(config['path']['tags'], all_tags)
    
    # 清空过期列表
    writefile(config['path']['nulltag'], [])


# ==================== 测试 ====================

def main():
    """测试函数"""
    # add_folder_tag()
    # init_input()
    # add_dead_tag()
    pass


if __name__ == '__main__':
    main()

