import os
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook


def get_detail_info_byid(job_id, outputfolder):
    '''分析每个职位页面详情'''

    if os.path.exists(outputfolder) is not True:
        os.mkdir(outputfolder)

    req_url = 'http://www.lagou.com/jobs/' + job_id + '.html'

    req = requests.get(req_url)
    html = req.content
    html_soup = BeautifulSoup(html, 'html.parser')

    job_bt_soup = html_soup.find('dd', class_='job_bt')

    # 加载用户自定义词典
    # jieba.load_userdict('C:/Users/XuLu/PycharmProjects/LagouJob/userdict.txt')

    # 加载停用词
    # jieba.analyse.set_stop_words('C:/Users/XuLu/PycharmProjects/LagouJob/stopwords.txt')

    # 这里是为了防止请求已失效的职位数据
    if job_bt_soup is not None:
        str_txt = job_bt_soup.text
        print(str_txt)

        result_txt_path = outputfolder + os.sep + job_id + '.txt'
        with open(result_txt_path, 'wt', encoding='utf-8') as f:
            f.write(str_txt)
            f.flush()
            f.close()
            print('职位编号为' + job_id + ' 的数据写出完成...')


def get_jobid_list(job_excel_file_path):
    '''从Excel文件里得到每个职位的职位ID，用来做进一步分析'''

    job_id_list = []
    wb = load_workbook(job_excel_file_path)
    ws = wb['职位信息']
    for rowindex in range(2, len(ws.rows) + 1):
        job_id = ws.cell(row=rowindex, column=4).value
        job_id_list.append(str(job_id))

    return job_id_list


if __name__ == '__main__':
    job_id_list = get_jobid_list('D:/网络爬虫.xlsx')
    outputdir = 'd:/'

    for each_job_id in job_id_list:
        get_detail_info_byid(each_job_id, 'D:/LagouJobInfo/lagou/details/网络爬虫')