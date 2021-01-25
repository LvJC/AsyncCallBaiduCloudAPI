import os
import asyncio
import logging
import base64

import glob
import json
import pandas as pd

from utils import IMAGE_CENSOR, CLASS_DICT, read_file, request, urlencode, fetch_token

async def fetch_result(base_url, imgpath):
    img = read_file(imgpath)
    # logging.info("Send request")
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(None, request, base_url, urlencode({'image': base64.b64encode(img)}))
    response = await future
    # logging.info("Return response")
    return imgpath, response

async def async_scan(base_url, input_path, output_path):
    samples_dir_list = glob.glob(input_path+'*_samples/')
    samples_dir_list.sort()
    
    samples_dir = samples_dir_list[0]
    samples_path_list = []
    from remove_corrupted_imgs import IMG_EXTENSIONS
    for type in IMG_EXTENSIONS:
        samples_path_list.extend(glob.glob(samples_dir+'**/*'+type, recursive=True))
    samples_path_list = samples_path_list[:]

    df = pd.DataFrame()
    step_size = 40 #
    n_samples = len(samples_path_list)
    for start_ind in range(0, n_samples, step_size):
        end_ind = start_ind + step_size
        if end_ind >= n_samples:
            end_ind = n_samples
        logging.info(f"*** Processing {start_ind} - {end_ind}")
        tasks = [ asyncio.create_task(fetch_result(base_url, path)) for path in samples_path_list[start_ind:end_ind] ]
        await asyncio.wait(tasks)
        result_list = [task.result() for task in tasks]
        imgpath_list, resp_list = zip(*result_list)
        for index in range(start_ind, end_ind):
            df.loc[index, 'image_path'] = imgpath_list[index-start_ind]
            try:
                data = json.loads(resp_list[index-start_ind])#["data"]
                # logging.info(data,imgname)
                df.loc[index, 'Labels'] = resp_list[index-start_ind] # json.dumps(data) # str
                if data['conclusion'] != "合规":
                    for lable_dict in data['data']:
                        df.loc[index, CLASS_DICT.get(lable_dict["msg"])] = lable_dict['probability']
            except Exception as e:
                df.loc[index, 'Labels'] = f"{e}"
                logging.info("Error msg:", imgpath_list[index-start_ind], e)
        df = export_results(df, output_path)
        

def export_results(df, output_path):
    (mode, header) = ('a',False) if os.path.exists(output_path) else ('w',True)
    df.to_csv(output_path, mode=mode, header=header, encoding="utf_8_sig")
    return df[0:0] # reset df, reduce memory usage


if  __name__ == "__main__":
    input_path = 'data/samples/nsfw_samples/'
    output_path = 'results/nsfw_samples.csv'
    token = fetch_token()
    base_url = IMAGE_CENSOR + "?access_token=" + token
    for class_key in list(CLASS_DICT.keys()):
        CLASS_DICT["疑似存在"+class_key+"不合规"] = CLASS_DICT.pop(class_key)
    asyncio.run( async_scan(base_url, input_path, output_path) ) 
