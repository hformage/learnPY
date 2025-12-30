import os
import heapq
from PIL import Image
from core import config


class ThumbnailMaker:
    """缩略图制作和拼接类（优化版）"""
    
    def __init__(self, tag, thumbnail_size=None, row_width=2400, rows_per_montage=3, max_images=90):
        self.tag = tag
        # 从配置读取缩略图尺寸
        if thumbnail_size is None:
            thumbnail_size = tuple(config.get('runtime', {}).get('THUMBNAIL_SIZE', [400, 400]))
        self.thumbnail_size = thumbnail_size
        self.row_width = row_width
        self.rows_per_montage = rows_per_montage
        self.max_images = max_images
        self.source_dir = os.path.join(config['path']['Gelbooru'], tag)
        self.target_dir = os.path.join(config['path']['Gelbooru'], 'sample')
        os.makedirs(self.target_dir, exist_ok=True)
    
    def get_images_sorted_by_time(self):
        """获取目录下最新的N张图片路径和文件名（优化：减少stat调用）"""
        if not os.path.exists(self.source_dir):
            return [], []
        
        image_list = []
        valid_extensions = {'.png', '.jpg', '.jpeg'}
        
        try:
            with os.scandir(self.source_dir) as entries:
                for entry in entries:
                    if entry.is_file():
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext in valid_extensions:
                            try:
                                image_list.append((entry.stat().st_mtime, entry.path, entry.name))
                            except Exception:
                                pass
        except Exception as e:
            print(f'无法扫描目录 {self.source_dir}: {e}')
            return [], []
        
        if not image_list:
            return [], []
        
        # 使用heapq.nlargest更高效地获取最新的N张
        if len(image_list) > self.max_images:
            image_list = heapq.nlargest(self.max_images, image_list)
        else:
            image_list.sort(reverse=True)
        
        return [item[1] for item in image_list], [item[2] for item in image_list]
    
    def create_thumbnail(self, image_path):
        """创建缩略图并转换为RGB格式"""
        try:
            with Image.open(image_path) as img:
                # 只在需要时缩放
                if img.width > self.thumbnail_size[0] or img.height > self.thumbnail_size[1]:
                    img.thumbnail(self.thumbnail_size, Image.Resampling.LANCZOS)
                # 只在需要时转换
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                return img.copy()
        except Exception:
            return None
    
    def split_into_rows(self, thumbnails, start_idx=0):
        """将缩略图分成多行，返回(行列表, 消耗的图片数)"""
        rows = []
        current_row = []
        current_width = 0
        consumed = 0
        
        for i in range(start_idx, len(thumbnails)):
            thumb = thumbnails[i]
            if current_width + thumb.width > self.row_width and current_row:
                rows.append(current_row)
                if len(rows) >= self.rows_per_montage:
                    return rows, consumed
                current_row = []
                current_width = 0
            
            current_row.append(thumb)
            current_width += thumb.width
            consumed += 1
        
        if current_row and len(rows) < self.rows_per_montage:
            rows.append(current_row)
        
        return rows, consumed
    
    def create_montage(self, rows, montage_index, first_image_name):
        """从行数据创建拼接图并保存"""
        total_images = sum(len(row) for row in rows)
        if total_images < 2:
            return None
        
        row_heights = [max(img.height for img in row) for row in rows]
        row_widths = [sum(img.width for img in row) for row in rows]
        canvas_width = max(row_widths)
        canvas_height = sum(row_heights)
        
        canvas = Image.new('RGB', (canvas_width, canvas_height))
        
        y = 0
        for row_idx, row in enumerate(rows):
            x = 0
            for img in row:
                canvas.paste(img, (x, y))
                x += img.width
            y += row_heights[row_idx]
        
        first_name_base = os.path.splitext(first_image_name)[0]
        output_filename = f'{self.tag}_{montage_index}_{first_name_base}.jpg'
        output_path = os.path.join(self.target_dir, output_filename)
        
        try:
            # 使用较高质量但不过度优化（optimize=False 更快）
            canvas.save(output_path, 'JPEG', quality=85)
            return output_path
        except Exception as e:
            print(f'{self.tag}: 保存失败: {e}')
            return None
    
    def delete_old_thumbnails(self):
        """删除该tag的所有旧缩略图"""
        deleted_count = 0
        prefix = self.tag + '_'
        prefix_len = len(prefix)
        
        try:
            with os.scandir(self.target_dir) as entries:
                for entry in entries:
                    name = entry.name
                    if (entry.is_file() and 
                        name.startswith(prefix) and 
                        name.lower().endswith('.jpg') and
                        len(name) > prefix_len and 
                        name[prefix_len].isdigit()):
                        try:
                            os.remove(entry.path)
                            deleted_count += 1
                        except Exception:
                            pass
        except Exception as e:
            print(f'{self.tag}: 扫描目录失败: {e}')
        
        if deleted_count > 0:
            print(f'{self.tag}: Delete {deleted_count}')
    
    def process(self):
        """执行完整流程：删除旧文件 -> 生成缩略图 -> 创建拼接图"""
        self.delete_old_thumbnails()
        
        image_paths, image_names = self.get_images_sorted_by_time()
        if not image_paths:
            return
        
        # 批量创建缩略图
        thumbnails = []
        valid_names = []
        
        for path, name in zip(image_paths, image_names):
            thumb = self.create_thumbnail(path)
            if thumb:
                thumbnails.append(thumb)
                valid_names.append(name)
        
        if len(thumbnails) < 2:
            return
        
        # 创建拼接图
        created_count = 0
        thumb_idx = 0
        
        while thumb_idx < len(thumbnails):
            rows, consumed = self.split_into_rows(thumbnails, thumb_idx)
            
            if sum(len(row) for row in rows) < 2:
                break
            
            if self.create_montage(rows, created_count + 1, valid_names[thumb_idx]):
                created_count += 1
            
            thumb_idx += consumed


def main(tag):
    """为指定tag创建缩略图拼接"""
    ThumbnailMaker(tag).process()


if __name__ == '__main__':
    main('cinamon_(cinamori)')
