from gevent import monkey
import sys

monkey.patch_all()

import os
import logging
import glob
import gevent
from gevent import pool
import shutil

from util import toolkit
from util import excelhelper
from util import analyser
from spider import lagouspider
from spider import jobdetailspider

logging.basicConfig()
log = logging.getLogger()
log.addHandler(logging.StreamHandler(sys.stdout))

req_url = 'http://www.lagou.com/jobs/positionAjax.json?'
headers = {'content-type': 'application/json;charset=UTF-8'}

def mkdir_if_need(data_root, dir_name):
    dir = os.path.join(data_root, dir_name)
    if not os.path.exists(dir):  # Not exist
        os.mkdir(dir)
    elif not os.path.isdir(dir):  # is NOT dir
        raise RuntimeError('{} is not dir.'.format(dir))
    else:  # exist already
        pass

def check_dir(data_root='./data', recreate=False):
    if recreate:
        shutil.rmtree(data_root)

    if not os.path.exists(data_root):
        os.mkdir(data_root)

    mkdir_if_need(data_root, 'brief')
    mkdir_if_need(data_root, 'detail')
    mkdir_if_need(data_root, 'excel')


def get_brief_data():
    configmap = toolkit.readconfig('./job.xml')
    p = pool.Pool(1024)

    for item, value in configmap.items():
        for job in value:
            mkdir_if_need('./data/brief', job.parameter)

            print('start crawl ' + str(job.parameter) + ' ...')
            p.spawn(lagouspider.scrapy, job.parameter)

    p.join()


def gen_excel():
    log.debug('start generating Excel file...')
    excelhelper.process('./data/brief/')
    log.debug('Done! Please check your result...')


def get_detail_data():
    p = pool.Pool(1024)

    for file in glob.glob('./data/excel/*.xlsx'):
        filename = os.path.basename(file)
        pure_filename = os.path.splitext(filename)[0]

        job_id_list = jobdetailspider.get_jobid_list(file)
        for each_job_id in job_id_list:
            p.spawn(jobdetailspider.get_detail_info_byid, each_job_id, os.path.join('./data/detail', pure_filename))


    p.join()


def run_analyze():
    for dir in glob.glob('./data/detail/*'):
        log.info('Analyze {} ...'.format(os.path.basename(dir)))

        content_txt = analyser.get_content(dir)
        analyser.analyse(content_txt, './stopwords.txt', './userdict.txt')

if __name__ == '__main__':
    check_dir(recreate=False)

    log.info('Get all brief data from lagou.')
    get_brief_data()

    log.info('Generate excel')
    gen_excel()

    log.info('Get all detail data from lagou.')
    get_detail_data()

    log.info('Analyze...')
    run_analyze()
