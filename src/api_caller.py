# coding=utf-8
import json
import time

from aip import AipContentCensor

CLASS_DICT = {
    "一般色情":     "General Porn",
    "卡通色情":     "ACG Porn",
    "SM":           "SM",
    "低俗":         "Vulgar",
    "儿童裸露":     "Kid Nudity",
    "艺术品色情":   "Artwork Porn",
    "性玩具":       "Sex toys",
    "男性性感":     "Male sexy",
    "自然男性裸露":  "Natural Male Nudity",
    "女性性感":     "Female Sexy",
    "卡通女性性感":  "ACG Female Sexy",
    "孕肚裸露":     "Pregnant Belly Nudity",
    "特殊类":       "Special Category",
    "臀部特写":     "Hip Close-up",
    "脚部特写":     "Foot Close-up",
    "裆部特写":     "Crotch Close-up",
    "亲密行为":     "Intimate Behavior",
    "卡通亲密行为":  "ACG Intimate Behavior",
}

BD_SUBLABEL_NAMES = list(CLASS_DICT.values())
BD_LABEL_NAMES = ['Porn', 'Sexy', 'Normal']


class BaiduAipCaller:
    def __init__(self, APP_ID, API_KEY, SECRET_KEY):
        self.client = AipContentCensor(APP_ID, API_KEY, SECRET_KEY)


    def predict(self, img):
        """
        Args:
            img (str): bytes
        Returns:
            Use str type in general
        !!!NOTICE: 
        Follow https://cloud.baidu.com/doc/ANTIPORN/s/jk42xep4e, 
            Image support: ~(5kb, 4M] and short side pixel~[128, 4096]
        """
        num_tries = 1
        while True:
            num_tries += 1
            try:
                resp_dict = self.client.imageCensorUserDefined(img)
                # ensure it has valid content
                if resp_dict['conclusion'] != "合规":
                    lable_dict = resp_dict['data'][0]
                    msg = lable_dict["msg"]
                    prob = lable_dict['probability']
                break
            except:
                time.sleep(0.1)
                if num_tries <= 5:
                    continue
                else:
                    resp_dict = f'*** cannot get the result for {num_tries} tries'
                    break
            
        return json.dumps(resp_dict, ensure_ascii=False)


if __name__ == '__main__':
    from utils import load_config, read_image_file
    import logging
    import os

    config = load_config("conf/bd_config.yaml")
    if not config or 'APP_ID' not in config or 'API_KEY' not in config or 'SECRET_KEY' not in config:
        logging.critical('Fail to load configuration file')
        os._exit(1)

    aip_caller = BaiduAipCaller(config['APP_ID'], config['API_KEY'], config['SECRET_KEY'])
    result = aip_caller.predict(read_image_file('data/logo.jpg'))
    print(result)
