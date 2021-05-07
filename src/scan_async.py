import numpy as np
import pandas as pd

from utils import read_image_file
from aip_caller import BaiduAipCaller, CLASS_DICT, BD_LABEL_NAMES, BD_SUBLABEL_NAMES

class AsyncScanner(object):
    def __init__(self, caller, image_path_list, label_dir, config) -> None:
        self.caller = caller
        self.image_path_list = image_path_list
        self.label_dir = label_dir
        self.config = config
        self.raw_resp_csv_path = os.path.join(self.label_dir, "raw.csv")

    async def fetch_result(self, imgpath):
        loop = asyncio.get_event_loop()
        img = read_image_file(imgpath)
        future = loop.run_in_executor(None, self.caller.predict, img) #img:bytes
        response = await future
        return imgpath, response

    async def async_scan(self):
        """
            Only save raw response message.
        """
        df = pd.DataFrame(columns=["image_path", "BDLabels"])
        step_size = 50
        n_samples = len(self.image_path_list)
        for start_ind in range(0, n_samples, step_size):
            end_ind = start_ind + step_size
            if end_ind >= n_samples:
                end_ind = n_samples
            logging.info(f"*** Processing {start_ind} - {end_ind}")
            tasks = [ asyncio.create_task(self.fetch_result(path)) for path in self.image_path_list[start_ind:end_ind] ]
            await asyncio.wait(tasks)
            result_list = [task.result() for task in tasks]
            imgpath_list, resp_list = zip(*result_list)
            for index in range(start_ind, end_ind):
                df.loc[index, 'image_path'] = imgpath_list[index-start_ind]
                try:
                    data = resp_list[index-start_ind]
                    df.loc[index, 'BDLabels'] = resp_list[index-start_ind] # json.dumps(data, ensure_ascii=False) if dict
                except Exception as e:
                    df.loc[index, 'BDLabels'] = f"{e}"
                    logging.error("Error msg:", imgpath_list[index-start_ind], e)
            df = self.export_results(df, self.raw_resp_csv_path)
            
    @staticmethod
    def export_results(df, output_path):
        (mode, header) = ('a',False) if os.path.exists(output_path) else ('w',True)
        df.to_csv(output_path, mode=mode, header=header, encoding="utf_8_sig", index=False)
        return df[0:0]

    #TODO: Generate label score using sublabel scores
    def postprocess(self):
        sublabel_output_path = os.path.join(self.label_dir, "sublabel.csv")
        self.postprocess_sublabel(sublabel_output_path)
        # label_output_path = os.path.join(self.label_dir, "label.csv")
        # self.postprocess_label(label_output_path)

    def postprocess_sublabel(self, output_path):
        df = pd.read_csv(self.raw_resp_csv_path, index_col=False)
        sublabel_df = pd.DataFrame(columns=['image_path']+BD_SUBLABEL_NAMES)
        for index in range(0, df.shape[0]):
            sublabel_df['image_path'] = df['image_path']
            try:
                data = json.loads(df.loc[index, 'BDLabels'])
                if data['conclusion'] != "合规":
                    for lable_dict in data['data']:
                        subclass_name = CLASS_DICT.get(lable_dict["msg"])
                        sublabel_df.loc[index, subclass_name] = lable_dict['probability']
            except Exception as e:
                logging.error(f"Error at index#{index}: {e}")
                for sublabel_name in BD_SUBLABEL_NAMES:
                    sublabel_df.loc[index, sublabel_name] = np.nan

        sublabel_df.drop_duplicates().dropna(subset=[BD_SUBLABEL_NAMES[0]]).to_csv(output_path, encoding="utf_8_sig", index=False)


if  __name__ == "__main__":
    import argparse
    from utils import load_config, get_io_info

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="samples/nsfw_samples/",
        help="path to input csv file (with column image_path) or data folder for inference",
    )
    parser.add_argument(
        "--out_folder",
        default="results/nsfw_samples/baidu/",
        help="folder for logging and consolidated results"
    )
    parser.add_argument(
        "--config_path",
        default="conf/bd_config.yaml",
        help="path to configuration file"
    )

    args = parser.parse_args()

    # logging config
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    config = load_config(args.config_path)
    
    input_dir = args.input
    output_dir = args.out_folder

    samples_path_list, label_dir = get_io_info(input_dir, output_dir)
    
    for class_key in list(CLASS_DICT.keys()):
        CLASS_DICT["疑似存在"+class_key+"不合规"] = CLASS_DICT.pop(class_key)

    bd_aip_caller = BaiduAipCaller(config['APP_ID'], config['API_KEY'], config['SECRET_KEY'])

    scanner = AsyncScanner(bd_aip_caller, samples_path_list[:], label_dir, config)
    asyncio.run( scanner.async_scan() )
    logging.info("***** Done for raw response collection.")
    scanner.postprocess()
