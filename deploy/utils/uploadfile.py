#!/usr/bin/env python
# -*- coding: utf-8 -*-


# install mysqldb :
# 1. sudo apt-get install python-pip python-dev libmysqlclient-dev
# 2. pip install mysqlclient
# install requests:
# pip install requests

"""
     Usage of this script : jenkins job

     uploadfile -u ${GERRIT_CHANGE_OWNER_NAME} u:g:d:o:f:p:b:j:
                -o old_version
                -g version_group
                -d description
                -f obj_file
                -p process_name
                -b branch
                -j job_name


     ${GERRIT_CHANGE_OWNER_NAME} is jenkins built-in env variable, refer to the gerrit commit change owner

"""

import requests
import sys
import getopt
import re
import MySQLdb
import datetime
from hashlib import sha1, md5
import io
import magic
import os
import functools
import time
import json
import base64
import hmac
import wget
from randstr import randstr
import commands
import shutil

PACKET_DEPLOY_ENDPOINT = "http://deploy.sysop.bigo.sg/jpack/api/version/add"
GET_BIN_TYPE_URL = "http://deploy.sysop.bigo.sg/jpack/api/getBinInfo"
API_CENTER_ENDPOINT = "http://auth.bigo.sg/api/v1/storage/genStoreUploadSig/"
DFS_ENDPOINT = "http://bfs.bigo.sg/file/new?bucket="
SYSTEM_ID = "10004"
OPERATOR = "guojiaxin"
KEY = "GnpLqmmhLWzye5bN"


def logInfoOnStartEnd(param):
    if not hasattr(param, '__call__'):
        assert isinstance(param, tuple)
        assert len(param) == 2

        def handleFunc(fun):
            @functools.wraps(fun)
            def handleArgs(*args, **kwargs):
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  " + fun.__name__ + "  " + param[0]
                fun(*args, **kwargs)
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  " + fun.__name__ + "  " + param[1]
                return

            return handleArgs

        return handleFunc
    else:
        def handlefunc(*args, **kwargs):
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " ==> start " + param.__name__
            result = param(*args, **kwargs)
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " ==> end " + param.__name__
            return result

        return handlefunc


def retry(retryTimes, EXIT=True):
    assert not hasattr(retryTimes, '__call__')
    assert isinstance(retryTimes, int)
    assert isinstance(EXIT, bool)

    def handleFunc(fun):
        @functools.wraps(fun)
        def retryforRetryTimes(*args, **kwargs):
            for i in range(retryTimes):
                try:
                    print fun.__name__, "retry times : ", i
                    result = fun(*args, **kwargs)
                    break
                except Exception as e:
                    print e
                    if i == retryTimes - 1:
                        if EXIT:
                            print "retry for", retryTimes, "times, but all error when exec", fun.__name__, " exit."
                            sys.exit(4)
                        else:
                            print "retry for", retryTimes, "times, but all error when exec", fun.__name__, " raise Execption."
                            raise ExecError()
            return result

        return retryforRetryTimes

    return handleFunc


@retry(2)
@logInfoOnStartEnd(("1qaz", "2wsx"))
def test(Error=False):
    time.sleep(2)
    if Error:
        raise AttributeError("err")


class ExecError(Exception):
    def __init__(self, s="ExecError"):
        self._s = s

    def __repr__(self):
        return str(self._s)


class ApiCenterError(Exception):
    def __init__(self, s="ApiCenterError"):
        self._s = s

    def __repr__(self):
        return str(self._s)


class DFSUploadError(Exception):
    def __init__(self, s="DFSUploadError"):
        self._s = s

    def __repr__(self):
        return str(self._s)


class AddVersionError(Exception):
    def __init__(self, s="AddVersionError"):
        self._s = s

    def __repr__(self):
        return str(self._s)


class FileSender(object):
    def __init__(self, bucket, packDeployUrl, tokenUrl, dfsUrl, filename):
        self._bucket = bucket
        self._packDeployUrl = packDeployUrl
        self._tokenUrl = tokenUrl
        self._file = filename
        self._dfsUrl = dfsUrl

    @staticmethod
    def get_md5(filename):
        m = md5()
        n = 4096
        fd = open(filename, 'rb')
        while True:
            buf = fd.read(n)
            if buf:
                m.update(buf)
            else:
                break
        fd.close()
        print 'md5sum: ', m.hexdigest()
        return m.hexdigest()

    @staticmethod
    def limitSize(f):
        fileSize = os.path.getsize(f)
        print 'filesize :', fileSize
        # [0,1024], <1Kb
        if fileSize < 1024:
            return [0, 1024]
        # [1024,1024*1024], 1kb ~ 1Mb
        elif fileSize >> 10 < 1024:
            return [1024, 1048576]
        # [1048576,1073741824] 1mb ~ 1gb
        elif fileSize >> 20 < 1024:
            return [1048576, 1073741824]
        #
        elif fileSize < 4294967296:
            return [1073741824, 4294967296]
        else:
            print 'file upload too large (more than 4GB)'
            sys.exit(4)

    def RestrictMIMEType(self):
        s = {"application/octet-stream", "application/x-binary", "application/x-executable"}
        s.add(magic.from_file(self._file, mime=True))
        return list(s)

    def SetExpireTime(self):
        # expire after 360 days.
        return int(time.time()) + 31104000

    @retry(2, EXIT=False)
    @logInfoOnStartEnd
    def get_token(self):
        policy = {
            "bucket": self._bucket,
            "expires": self.SetExpireTime(),
            "fsizeLimit": self.limitSize(self._file),
            "md5": self.get_md5(self._file)
        }
        h = {'Content-Type': 'application/json'}
        d = {
            "systemid": SYSTEM_ID,
            "operator": OPERATOR,
            "policy": policy
        }

        # rsp = requests.post("http://auth.bigo.sg/api/v1/storage/genStoreUploadSig/", json=d, headers=h)
        rsp = requests.post(self._tokenUrl, json=d, headers=h, timeout=30)
        if rsp.status_code != 200 or rsp.json().get('code') != 0:
            print rsp.status_code
            raise ApiCenterError()
        token = rsp.json().get('data')
        return token

    @retry(2)
    @logInfoOnStartEnd
    def UploadDFS(self, token):
        header = {"Content-Type": "application/octet-stream", "Authorization": str(token),
                  "Content-Length": str(os.path.getsize(self._file))}
        with io.open(self._file, 'rb',
                     buffering=io.DEFAULT_BUFFER_SIZE * 10) as fd:
            rq = requests.post(headers=header, url=self._dfsUrl + self._bucket,
                               data=fd.read(), timeout=300)
            print rq.status_code
            if rq.status_code != 200 or not rq.json().get('url') or rq.json().get('url') == "":
                print rq.content
                raise DFSUploadError()


            return rq.json().get('url')

    @logInfoOnStartEnd
    def gen_file_sign(self, key):  ## key -> string  policy -> json

        policy = {
            "bucket": self._bucket,
            "expires": self.SetExpireTime(),
            "fsizeLimit": self.limitSize(self._file),
            "md5": self.get_md5(self._file)
        }
        policy_str = json.dumps(policy)

        encodedPutPolicy = base64.urlsafe_b64encode(policy_str)

        encodedSign = base64.urlsafe_b64encode(hmac.new(key, encodedPutPolicy, sha1).digest())

        signPolicy = encodedSign + ':' + encodedPutPolicy

        token = "BIGO" + " " + signPolicy

        return token

    @retry(3)
    @logInfoOnStartEnd((" start...", " finish... "))
    def addVersion(self, user, group, old, new, desc, process, bin_url, bintype, md5, size,mime):
        # username = user #"yanzhejing"
        # version_group = group #"admintest"
        # old_version = old #"vava1.1"
        # new_version = new #"201809101701"
        # description = desc #"test添加版本test接口test"
        # process_name = process #"yzj0910p1"
        _bin_type = ''
        if bintype in ['bin','file']:
            _bin_type = ''
        elif bintype in ['tar','tar.gz','gz']:
            _bin_type = '.tar.gz'
        data = {
            "username": user,
            "password": sha1(user + 'jenkins').hexdigest(),
            "version_group": group,
            "old_version": old,
            "new_version": new,
            "description": desc,
            "process_name": process,
            "bin_url": bin_url,
            "bin_type": bintype,
            "bin_info": json.dumps({"url":bin_url,"mime":mime,"md5":str(md5),"size":int(size),"ts":int(time.time())}),
            "file_name": group + '_' + new + _bin_type
        }
        # 添加版本接口

        response = requests.post(self._packDeployUrl, data=data, timeout=60)
        print ("add version api return content: ", response.status_code, json.dumps(response.json(), encoding="UTF-8"))
        if response.status_code != 200 or not response.json().get('ret'):
            raise AddVersionError()


@retry(2)
@logInfoOnStartEnd
def getTarOrbin(username, password, version_group, version):
    data = {
        "username": username,
        "password": password,
        "version_group": version_group,
        "version": version
    }
    ret = requests.post(GET_BIN_TYPE_URL, data=data, timeout=60)
    print ret.status_code
    print ret.content
    print ("get bin type api return content: ", ret.status_code, json.dumps(ret.json(), encoding="UTF-8"))
    if ret.status_code != 200 or not ret.json().get('binType'):
        raise AddVersionError()
    return ret.json().get('binType'), ret.json().get('url')


@retry(2)
@logInfoOnStartEnd
def downloadTarReplace(url, version_group, version, f):  # f is the file to replace and upload
    rstr = randstr()
    path = "/data/tmp/" + version_group + '_' + version + '_' + rstr
    os.mkdir(path)
    os.chdir(path)
    filename = wget.download(url, out=version_group + '_' + version + '_' + rstr + '.tar.gz')
    status, output = commands.getstatusoutput('tar -xf ' + filename + ' && cp ' +
                             f + ' ./ && rm --preserve-root ' + filename + ' && tar -zcf ' + filename + ' * ')
    if status != 0:
        print 'status not zero'
        sys.exit(9)
    print ('download file and replace status, output :', status, output)
    return path, filename


def logtime(msg):
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " ==> " + msg


class Db_access(object):
    def __init__(self):
        self._conn, self.cursor = None, None
        self._connect()

    def _connect(self):
        try:
            self._conn = MySQLdb.connect(host='139.5.108.203', db='reviewdb', user='gerrit',
                                         passwd='20S31Q7s', port=6308, charset='utf8', connect_timeout=10)

            self.cursor = self._conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        except Exception as e:
            self.msg = 'Error when connect mysql: %s' % e.args
            print (self.msg)
            sys.exit(9)

    def connect(self):
        try:
            if not self._conn.ping():
                print ('Reconnect mysql...')
                self._connect()
        except Exception:
            print ('Reconnect mysql...')
            self._connect()

    def get(self, sql):
        self.connect()
        # print sql
        try:
            self.cursor.execute(sql)

            raw_records = self.cursor.fetchall()

            return 0, list(raw_records)
        except Exception as e:
            print (e.args)
            return 1, []
        finally:
            self._conn.commit()
            self.cursor.close()

    def mod(self, sql):
        self.connect()
        # print sql
        try:
            if not isinstance(sql, list):
                _sql = [sql]
            else:
                _sql = sql
            for _s in _sql:
                if _s:
                    self.cursor.execute(_s)
            c = self._conn.commit()
            return 0, c
        except Exception as e:
            print (str(e.args))
            print (sql)
            return 2, 'sql error'
        finally:
            self.cursor.close()


def get_fixs(s):
    try:
        last_dot_index = s.rindex('.')
        prefix = s[0:last_dot_index]
        print 'prefix := ', prefix
        suffix = s[last_dot_index + 1:]
        print 'suffix := ', suffix
        if not suffix.isdigit():
            print 'suffix not digit, exit.'
            sys.exit(9)
    except Exception as e:
        print 'format error: ', e
        sys.exit(9)
    return prefix, suffix


def process(user, group, desc, obj, process, job, old=None):
    IF_INITIAL_UPLOAD = False
    get_list_url = "http://deploy.sysop.bigo.sg/jpack/api/version/list"
    username = user
    version_group = group
    description = desc
    obj_file = obj
    process_name = process

    db = Db_access()
    query_latest_version_sql = "select latestversion from jenkins_job_version where jobname = '%s'" % job
    logtime("start get latest version sql")
    s, c = db.get(query_latest_version_sql)
    if s:
        print 'query latest version failed...'
        sys.exit(9)
    print 'get latest version from db ok...'
    logtime("end get latest version sql")

    logtime("start get versionlist ")
    try:
        rq = requests.get(get_list_url,
                          params={"username": username, "password": sha1(username + 'jenkins').hexdigest(),
                                  "version_group": version_group}, timeout=60)
        print ("get version list content : ==>", rq.content)
        version_list_got = rq.json().get('msg')
    except Exception as e:
        print e
        logtime("end get versionlits, has exception")
        sys.exit(9)
    logtime("end get versionlist,ok.")
    if old:  # 传入了old版本

        if not c:  # db 为空，首次提交
            IF_INITIAL_UPLOAD = True
        if old not in version_list_got:
            print 'old version not in version list ,error , exiting...'
            sys.exit(9)
        print 'old version exist, ok....'
        old_version = str(old)
        print 'old version info =====>'
        ov_prefix, ov_suffix = get_fixs(old_version)
        # if c: #数据库非空
        #  new_version = datetime.datetime.now().strftime('%Y%m%d') + '_' + str(int(c[0].get('latestversion')) + 1)
        # db_version = c[0].get('latestversion')
        # print 'db version info =====>'
        # db_prefix , db_suffix = get_fixs(str(db_version))
        if ov_prefix + '.' + str(int(ov_suffix) + 1) in version_list_got:
            print 'newer version already exist :', ov_prefix + '.' + str(int(ov_suffix) + 1)
            sys.exit(9)

        print 'newer version not found, its ok...'
        old_version = old
        new_version = ov_prefix + '.' + str(int(ov_suffix) + 1)
        print 'new version : ', new_version

    else:  # 传入为空
        if not c:
           print 'no any version specified, but also no previous version'
           sys.exit(9)
        db_version = str(c[0].get('latestversion'))
        print 'db version info =====>'
        _new_version = ''
        db_prefix, db_suffix = get_fixs(str(db_version))
        if str(db_version) != version_list_got[0]:
            if db_prefix + '.' + str(int(db_suffix) - 1) == version_list_got[0] and str(db_version) not in version_list_got:
                _new_version = db_version
            else:
                print 'jenkins latest version: ', db_version, 'not equal to the packDeploy latest version:', \
                version_list_got[0]
                sys.exit(8)
        if db_prefix + '.' + str(int(db_suffix) + 1) in version_list_got:
            print 'newer version already exist :', db_prefix + '.' + str(int(db_suffix) + 1)
            sys.exit(9)
        if _new_version != '':
            new_version = _new_version
        else:
            new_version = db_prefix + '.' + str(int(db_suffix) + 1)
        old_version = db_version
        print 'new version : ', new_version
    # get tar.gz/bin
    binType ,url = getTarOrbin(username, sha1(username + 'jenkins').hexdigest(), version_group, old_version)
    print ('bintype :', binType, 'url:', url)
    if binType in ["bin","file"]:
        fs = FileSender("jpack", PACKET_DEPLOY_ENDPOINT, API_CENTER_ENDPOINT, DFS_ENDPOINT, obj_file)
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
        fs.addVersion(username, version_group, old_version, new_version, description, process_name, bin_url, "bin",_md5,_size,_mime)
    elif binType == "tar.gz":
        path, obs_filename = downloadTarReplace(url, version_group, old_version, obj_file)
        print ('file to replace :', obj_file)
        # obj_flie == file to replace
        # path+'/'+obs_filename=file replaced, to upload, its tarball.
        fs = FileSender("jpack", PACKET_DEPLOY_ENDPOINT, API_CENTER_ENDPOINT, DFS_ENDPOINT, path + '/' + obs_filename)
        _md5 = fs.get_md5(path + '/' + obs_filename)
        _size = os.path.getsize(path + '/' + obs_filename)
        _mime = magic.from_file(path + '/' + obs_filename, mime=True)
        print ('file that has been replaced and ready to upload, should be tarball')
        try:
            token = fs.get_token()
        except Exception as e:
            print e
            print 'api center get token error, self generate instead..'
            token = fs.gen_file_sign(KEY)
        bin_url = fs.UploadDFS(token)
        fs.addVersion(username, version_group, old_version, new_version, description, process_name, bin_url, binType,_md5,_size,_mime)
        shutil.rmtree(path)
    else:
        print 'unknown bintype:',binType
        sys.exit(9)

    # 收尾,更新数据库
    # lv = int(new_version.split('_')[1])
    logtime("start update sql")
    if not IF_INITIAL_UPLOAD:  # 不是首次

        update_sql = "update jenkins_job_version set latestversion ='%s' where  jobname = '%s'" % (new_version, job)
        status, result = db.mod(update_sql)
    else:  # 首次
        insert_sql = "insert into jenkins_job_version (jobname , latestversion) VALUES ('%s','%s')" % (job, new_version)
        status, result = db.mod(insert_sql)
    if status:
        print 'critical!!! Database update failed, must fix it right now!!!'
        sys.exit(9)
    print 'database update success....finish all....'
    logtime("end update sql")


if __name__ == '__main__':
    u, g, d, o, f, p, j = "", "", "", "", "", "", ""
    try:
        options, args = getopt.getopt(sys.argv[1:], "u:g:d:o:f:p:j:")
        for k, v in options:
            if k == '-u':
                u = v
            elif k == '-g':
                g = v
            elif k == '-o':
                o = v
            elif k == '-d':
                d = v
            elif k == '-p':
                p = v
            elif k == '-b':
                b = v
            elif k == '-j':
                j = v
            else:
                f = v
        print 'username : ', u
        print 'version_group :', g
        print 'description : ', d
        print 'old version: ', o
        print 'file : ', f
        print 'process_name : ', p
        print 'jobname : ', j
        process(u, g, d, f, p, j, o)
    except getopt.GetoptError as err:
        print "\033[0;31mArg input error\033[0m", err
        sys.exit(9)