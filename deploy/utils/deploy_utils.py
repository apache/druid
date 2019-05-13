import requests
import sys
import hashlib
import json
import os
import magic

current_path = os.path.abspath(__file__)
father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".")
print father_path
sys.path.append(father_path)
import uploadfile

PACKET_DEPLOY_ENDPOINT = "http://deploy.sysop.bigo.sg/jpack/api/version/add"
GET_BIN_TYPE_URL = "http://deploy.sysop.bigo.sg/jpack/api/getBinInfo"
API_CENTER_ENDPOINT = "http://auth.bigo.sg/api/v1/storage/genStoreUploadSig/"
DFS_ENDPOINT = "http://bfs.bigo.sg/file/new?bucket="
SYSTEM_ID = "10004"
OPERATOR = "guojiaxin"
KEY = "GnpLqmmhLWzye5bN"
global confs

def get_conf(path):
    fd = open(path)
    confs = {}
    for line in fd:
        conf = line.replace("\n", "")
        pair = conf.split("=")
        if len(pair) == 2:
            confs[pair[0]] = pair[1];
    return confs

def parse_jenkins_param(para):

    result = {}
    words = para.split('+++++')
    for word in words:
        pair = word.split('===')
        if len(pair) == 2:
            result[pair[0]] = pair[1]
    return result

def get_old_version(user, group):

    url = 'http://deploy.sysop.bigo.sg/jpack/api/version/list'
    d = {
        'username':user,
        'version_group':group,
        'password':hashlib.sha1(user + 'jenkins').hexdigest()
    }
    print 'params:', d
    r = requests.get(url, params=d, timeout=60)
    re = str(r.text)
    print re
    result = json.loads(re)
    return result['msg'][0]

# pattern: xxx.yyy.num
def gen_new_version(old_version):

    words = old_version.split('.')
    last_word = words[len(words) - 1]
    version_num = int(last_word)
    version_prefix = old_version[:-len(last_word)]
    return version_prefix + str(version_num + 1)

def main_process(obj_file, username, version_group,
                 old_version, description, process_name):

    new_version = gen_new_version(old_version)

    fs = uploadfile.FileSender("jpack", PACKET_DEPLOY_ENDPOINT,
                               API_CENTER_ENDPOINT, DFS_ENDPOINT, obj_file)
    _md5 = fs.get_md5(obj_file)
    _size = os.path.getsize(obj_file)
    _mime = magic.from_file(obj_file, mime=True)
    try:
        token = fs.get_token()
    except Exception as e:
        print e
        print 'api center get token error, self generate instead..'
        token = fs.gen_file_sign(KEY)
    bin_url = fs.UploadDFS(token)
    fs.addVersion(username, version_group, old_version, new_version,
                  description, process_name, bin_url, "bin",_md5,_size,_mime)

def main(confs, jenkins_param, bin_dir):
    version_group = confs['version_group']
    process_name = confs['process_name']
    user = jenkins_param['PACKET_USER']
    version_desc = jenkins_param['VERSION_DESC']

    old_version = get_old_version(user, version_group)
    main_process(bin_dir, user, version_group, old_version,
                 version_desc, process_name)


