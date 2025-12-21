import os,sys
from PIL import Image
import datetime


def loginfo(txt,end='\n'):
    print(txt,end=end)

def search_large_images(directory, size):
    image_list = {}
    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        if os.path.isfile(path) and filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            try:
                image_list[path] = os.path.getmtime(path)
            except:
                filesize = os.path.getsize(path)/((1024 * 1024))
                loginfo(f'open {path} failed, size {filesize}')
    return image_list


def resize_image(image_path,size):
    try:
        img = Image.open(image_path)
        img.thumbnail(size)
        img = img.convert('RGB')
        return img
    except:
        loginfo(f'resize {image_path} failed')
        return None

def save_image(target_path, image_list):
    filename = ''
    global img_list
    global name_list
    global image_list1
    global image_list2
    global image_list3
    global line1
    global width1 
    global width2 
    global width3 
    global height1
    global height2
    global height3
    line1 = 0
    img_list = []
    name_list = []
    image_list1 = []
    image_list2 = []
    image_list3 = []
    width1 = 0
    width2 = 0
    width3 = 0
    height1 = 0
    height2 = 0
    height3 = 0
    for image_name in image_list:
        tag = os.path.split(os.path.split(image_name)[0])[1]
        filename = os.path.split(image_name)[1]
        try:
            filename = tag + '_' + filename
            savename = os.path.join(target_path,filename)
            name_list.append(savename)
            img = resize_image(image_name,(400,400))
            if img:
                img_list.append(img)
        except:
            loginfo(f'save {image_name} failed')
        past_img(0)
    if img_list and len(img_list)>=2:
        past_img(1)

def past_img(savetag = 0):
    global img_list
    global name_list
    global image_list1
    global image_list2
    global image_list3
    global line1
    global width1 
    global width2 
    global width3 
    global height1
    global height2
    global height3
    global add_list
    widthsize = 2400
    if savetag == 1:
        if not image_list1:
            width1 = sum(map(lambda img: img.width, img_list))
            height1 = max(map(lambda img: img.height, img_list), default=0)
            image_list1 = img_list
        else:
            if not image_list2:
                width2 = sum(map(lambda img: img.width, img_list))
                height2 = max(map(lambda img: img.height, img_list), default=0)
                image_list2 = img_list
            else:
                if not image_list3:
                    width3 = sum(map(lambda img: img.width, img_list))
                    height3 = max(map(lambda img: img.height, img_list), default=0)
                    image_list3 = img_list

    if not image_list1:
        line1 += 1
        width1 = sum(map(lambda img: img.width, img_list))
        if width1 >widthsize:
            image_list1 = img_list[:line1-1]
            width1 = sum(map(lambda img: img.width, image_list1))
            height1 = max(map(lambda img: img.height, image_list1), default=0)
            img_list = img_list[line1-1:]
            line1 = 1
            image_list2 = []
            image_list3 = []
    else:
        if not image_list2:
            if img_list:
                line1 += 1
                width2 = sum(map(lambda img: img.width, img_list))
                if width2 >widthsize:
                    image_list2 = img_list[:line1-1]
                    width2 = sum(map(lambda img: img.width, image_list2))
                    height2 = max(map(lambda img: img.height, image_list2), default=0)
                    img_list = img_list[line1-1:]
                    line1 = 1
                    image_list3 = []
        else:
            if not image_list3:
                if img_list:
                    line1 += 1
                    width3 = sum(map(lambda img: img.width, img_list))
                    if width3 >widthsize:
                        image_list3 = img_list[:line1-1]
                        width3 = sum(map(lambda img: img.width, image_list3))
                        height3 = max(map(lambda img: img.height, image_list3), default=0)
                        img_list = img_list[line1-1:]
                        line1 = 1
                        savetag = 2
    if savetag != 0:
        width = max(width1,width2,width3)
        height = height1 + height2 + height3
        new_img = Image.new('RGB', (width, height))
        offset1 = 0
        offset2 = 0
        offset3 = 0
        cnt = 0
        for img in image_list1:
            cnt += 1
            new_img.paste(img, (offset1,0))
            offset1 += img.width
        for img in image_list2:
            cnt += 1
            new_img.paste(img, (offset2,height1))
            offset2 += img.width
        for img in image_list3:
            cnt += 1
            new_img.paste(img, (offset3,height1+height2))
            offset3 += img.width
        filename = os.path.splitext(name_list[0])[0]+'.jpg'
        add_list.add(filename)
        new_img.save(filename)
        width1 = 0
        width2 = 0
        width3 = 0
        height1 = 0
        height2 = 0
        height3 = 0
        image_list1 = []
        image_list2 = []
        image_list3 = []
        name_list = name_list[cnt:]
        if savetag == 1:
            img_list = []

def search(source, target,size, ind=0):
    for root, dirs, files in os.walk(source):
        if root != r'F:\Pic\Gelbooru':
            image_list = search_large_images(root, size)
            sorted_dict = dict(sorted(image_list.items(), key=lambda item: item[1], reverse=True))
            imgs = [a for a in sorted_dict.keys()][:90]
            save_image(target, imgs)

def deleteimg(tag,target_path,add_list):
    del_list = set()
    for root, dirs, files in os.walk(target_path):
        for file in files:
            if tag in file:
                del_list.add(os.path.join(root,file))
    cnt = 0
    for del_file in del_list:
        if del_file not in add_list:
            cnt += 1
            os.remove(del_file)
    loginfo(f'{tag} delete {cnt}')


def main(tag):
    global add_list
    add_list = set()
    size = (0, 0)
    target_path = r'F:\Pic\Gelbooru\sample'
    source = r'F:\Pic\Gelbooru' +'\\' + tag

    search(source, target_path,size, 0)
    deleteimg(tag,target_path,add_list)

if __name__=='__main__':
    main('an-telin')