import os
import sys
from config import config
import set_tag
import downloadpic
import threading
import concurrent.futures
import time
import glob

if __name__ == '__main__':
    select = sys.argv[1]
    #select = '3'

    if select == '1':
        #down pic
        set_tag.add_folder_tag()
        set_tag.init_input()
        taglist = set_tag.read_tagjson()
        tasks = []
        for tag in taglist:
            #taglist[tag]['status'] = 0 initial
            #taglist[tag]['status'] = 1 inprogress
            #taglist[tag]['status'] = 2 done
            #taglist[tag]['status'] = 7/8 downall tag inprogress/done
            #taglist[tag]['status'] = 9 exclude
            if taglist[tag]['status'] not in [1, 7, 8, 9]:
                tasks.append((tag, taglist[tag]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(downloadpic.down_single, tag, taglist): tag for tag, taglist in tasks}

        for future in concurrent.futures.as_completed(futures):
            return_list = future.result()
            download = downloadpic.Download()
            download.wait_to_process(None, return_list, 0)

    elif select == '2':
        #continue down
        set_tag.add_folder_tag()
        set_tag.init_input()
        taglist = set_tag.read_tagjson()
        tasks = []
        for tag in taglist:
            #taglist[tag]['status'] = 0 initial
            #taglist[tag]['status'] = 1 inprogress
            #taglist[tag]['status'] = 2 done
            #taglist[tag]['status'] = 7/8 downall tag inprogress/done
            #taglist[tag]['status'] = 9 exclude
            if taglist[tag]['status'] not in [7, 8, 9]:
                tasks.append((tag, taglist[tag]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(downloadpic.down_single, tag, taglist): tag for tag, taglist in tasks}

        for future in concurrent.futures.as_completed(futures):
            return_list = future.result()
            download = downloadpic.Download()
            download.wait_to_process(None, return_list, 0)

    elif select == '3':
        #down all tag
        set_tag.add_folder_tag()
        set_tag.init_input(1)
        set_tag.add_dead_tag()
        taglist = set_tag.read_tags()
        downloadpic.down_all_tags(taglist, 6)

    elif select == '4':
        #delete donw tag
        set_tag.del_input_done()
