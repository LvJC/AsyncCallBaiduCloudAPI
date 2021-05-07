from urllib.request import urlopen
import base64
import logging


def read_image_file(path):
    f = None
    try:
        f = open(path, 'rb')
        return f.read()
    except:
        logging.warning('Failed to read image file at %s', path)
        return None
    finally:
        if f:
            f.close()


def read_internet_image(url):
    try:
        data = urlopen(url)
        return data.read()
    except:
        logging.warning('Failed to retrieve image at %s', url)
        return None


def img2base64(imgpath):
    with open(imgpath, "rb") as f:
        encoded_str = base64.b64encode(f.read()).decode("utf-8")
    return encoded_str

def load_config(config_path):
    import yaml
    if not os.path.exists(config_path):
        logging.critical('Error: config file does not exist at %s', config_path)
        return {}

    try:    
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config
    except:
        logging.critical('Error: fail to load config file from %s', config_path, exc_info=True)
        return {}
    
 def get_io_info(data_path, out_folder):
    import pandas as pd
    from glob import glob
    image_path_list = []
    dataset_name = ''

    # Process .csv file with image urls or paths
    if data_path.endswith('.csv') and os.path.isfile(data_path):
        dataset_name = os.path.basename(data_path).split('.')[0]
        image_path_list = pd.read_csv(data_path, low_memory=False)['image_path'].dropna().drop_duplicates().to_list()
    # Process a folder of image files
    elif os.path.isdir(data_path):
        dataset_name = os.path.basename(data_path)
        [image_path_list.extend(glob(os.path.join(data_path, '**/*'+ext), recursive=True)) for ext in IMG_EXTENSIONS]
    else:
        logging.error('%s is not a valid input data source', data_path)
    
    tgt_folder = os.path.join(out_folder, dataset_name)
    if not check_create_dirs(tgt_folder):
        logging.critical('Fail to create output folder at %s', tgt_folder)
        tgt_folder = None
    else:
        result_path = os.path.join(tgt_folder, 'raw.csv')
        if image_path_list and os.path.isfile(result_path):
            processed_image_path_set = set(pd.read_csv(result_path, low_memory=False)['image_path'].to_list())
            image_path_list = [x for x in image_path_list if x not in processed_image_path_set]

    image_path_list.sort()
    return image_path_list, tgt_folder
