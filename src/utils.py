import sys
import json
import base64

# Ensure compatibility with python2 and python3
IS_PY3 = sys.version_info.major == 3
if IS_PY3:
    from urllib.request import urlopen
    from urllib.request import Request
    from urllib.error import URLError
    from urllib.parse import urlencode
    from urllib.parse import quote_plus
else:
    import urllib2
    from urllib import quote_plus
    from urllib2 import urlopen
    from urllib2 import Request
    from urllib2 import URLError
    from urllib import urlencode

APPID = '...' # Enter your APPID
API_KEY = '...' # Enter your API_KEY
SECRET_KEY = '...' # Enter your SECRET_KEY
IMAGE_CENSOR = "https://aip.baidubce.com/rest/2.0/solution/v1/img_censor/v2/user_defined"
TEXT_CENSOR = "https://aip.baidubce.com/rest/2.0/solution/v1/text_censor/v2/user_defined"

"""  TOKEN start """
TOKEN_URL = 'https://aip.baidubce.com/oauth/2.0/token'

""" All classes from Baidu antiporn """
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

"""
    Fetch token
"""
def fetch_token():
    params = {'grant_type': 'client_credentials',
              'client_id': API_KEY,
              'client_secret': SECRET_KEY}
    post_data = urlencode(params)
    if (IS_PY3):
        post_data = post_data.encode('utf-8')
    req = Request(TOKEN_URL, post_data)
    try:
        f = urlopen(req, timeout=5)
        result_str = f.read()
    except URLError as err:
        print(err)
    if (IS_PY3):
        result_str = result_str.decode()

    result = json.loads(result_str)

    if ('access_token' in result.keys() and 'scope' in result.keys()):
        if not 'brain_all_scope' in result['scope'].split(' '):
            print ('please ensure has check the  ability')
            exit()
        return result['access_token']
    else:
        print ('please overwrite the correct API_KEY and SECRET_KEY')
        exit()

"""
    Read file
"""
def read_file(image_path):
    f = None
    try:
        f = open(image_path, 'rb')
        return f.read()
    except:
        print('read image file fail')
        return None
    finally:
        if f:
            f.close()

"""
    Read image from your CDN
"""
def read_innernet_image(url):
    try:
        data = urlopen(url)
        return data.read()
    except:
        print(f'cannot access url: {url}')

"""
    Call remote service
"""
def request(url, data):
    req = Request(url, data.encode('utf-8'))
    has_error = False
    try:
        f = urlopen(req)
        result_str = f.read()
        if (IS_PY3):
            result_str = result_str.decode('utf-8')
        return result_str
    except  URLError as err:
        print(err)
