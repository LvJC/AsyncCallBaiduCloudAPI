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
