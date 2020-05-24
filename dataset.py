# dataset.py
# 数据集建立
# python 3.x

import sqlite3
import json
from datetime import date, timedelta
import logging
from pandas import read_sql_query
from tqdm import tqdm
from google.oauth2 import service_account
import pandas_gbq

# 提取PageRank值
SQQUERY_PGR = """
    SELECT user_id id,
        pagerank VAL_U_PGR
    FROM {:s}
"""
# 提取trueSkill值
SQQUERY_TS = """
    SELECT user_id id,
        ts VAL_U_TRS
    FROM {:s}
    WHERE judge_date IS NOT NULL
"""
# 建立特征数据表
TABLE = """
    CREATE TABLE {:s} (
    id INTEGER PRIMARY KEY NOT NULL,
    EXPERT_SCORE INTEGER NOT NULL,
    CNT_A INTEGER NOT NULL,
    AVG_A_SCORE REAL NOT NULL,
    MAX_A_SCORE INTEGER NOT NULL,
    SUM_A_SCORE INTEGER NOT NULL,
    MED_A_SCORE INTEGER NOT NULL,
    AVG_A_SPAN REAL NOT NULL,
    MAX_A_SPAN INTEGER NOT NULL,
    SUM_A_SPAN INTEGER NOT NULL,
    MED_A_SPAN INTEGER NOT NULL,
    CNT_T REAL DEFAULT 0.0,
    SUM_T_SCORE REAL DEFAULT 0.0,
    MAX_T_SCORE REAL DEFAULT 0.0,
    SUM_T_SPAN INTEGER NOT NULL,
    MAX_T_SPAN INTEGER NOT NULL,
    AVG_A_ENTRO REAL NOT NULL,
    MAX_A_ENTRO REAL NOT NULL,
    SUM_A_ENTRO REAL NOT NULL,
    MED_A_ENTRO REAL NOT NULL,
    AVG_A_CNT REAL NOT NULL,
    MAX_A_CNT INTEGER NOT NULL,
    SUM_A_CNT INTEGER NOT NULL,
    MED_A_CNT INTEGER NOT NULL,
    AVG_A_LEN REAL NOT NULL,
    MAX_A_LEN INTEGER NOT NULL,
    SUM_A_LEN INTEGER NOT NULL,
    MED_A_LEN INTEGER NOT NULL,
    CNT_Q REAL DEFAULT 0.0,
    SUM_Q_SPAN REAL DEFAULT 0.0,
    MAX_Q_SPAN REAL DEFAULT 0.0,
    CNT_C REAL DEFAULT 0.0,
    SUM_C_SPAN REAL DEFAULT 0.0,
    MAX_C_SPAN REAL DEFAULT 0.0,
    VAL_U_PGR REAL DEFAULT 0.0,
    VAL_U_TRS REAL DEFAULT 0.0
    );
"""
# 标签特殊字符转义
def tra(tag):
    return tag.replace('_sharp', '#').replace('_plus', '++')

# SQL文件读取
def query(file):
    return ''.join(open(file, 'r').readlines())

# 数据集构建类
class DatasetGenerator:
    # 初始化
    def __init__(self):
        with open('auth.json', 'r') as auth_file:
            cred = json.load(auth_file)
            self.credentials = service_account.Credentials.from_service_account_info(cred)
        self.tags = ['css', 'javascript', 'java', 'python', 'c_sharp', 'php',
                     'android', 'c_plus', 'html', 'jquery']
        self.data = sqlite3.connect("Data/StackExpert.sqlite")
        logger = logging.getLogger('pandas_gbq')
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler())

    # 析构函数
    def __del__(self):
        self.data.commit()
        self.data.close()

    # 预处理
    def preprocessing(self):
        # 选取当前日期的前600+30(数据库更新周期)日之前的所有数据作为数据读取范围
        arg = {'period': 120, '2,':'{2,}'}
        arg['udate'] = (date.today()-timedelta(630)).isoformat()
        arg['adate'] = (date.today()-timedelta(630-arg['period'])).isoformat()
        print('Source table generating...')
        # 生成分数数据表
        pandas_gbq.read_gbq(query('Script/Score.sql').format(**arg),
                            credentials=self.credentials)
        print('Score table generate succssfully...')
        # 生成排名数据表
        pandas_gbq.read_gbq(query('Script/Rank.sql').format(**arg),
                            credentials=self.credentials)
        print('Rank table generate succssfully...')

    # 数据集生成
    def dataset_gen(self):
        arg = {'period': 120}
        # 关联计算完成的Pageank和TrueSkill数据库
        pgr = sqlite3.connect('Data/PageRank.sqlite')
        trs = sqlite3.connect('Data/TrueSkill.sqlite')
        for tag in tqdm(self.tags):
            arg['tag'] = tag
            # 生成除了回答特征以外其他特征的数据
            features = pandas_gbq.read_gbq(query('Script/Dataset.sql').format(**arg),
                                           credentials=self.credentials).set_index('id')
            # 关联PageRank和TrueSkiil特征
            features = features.join(read_sql_query(
                SQQUERY_PGR.format(tag), pgr, index_col='id'))
            features = features.join(read_sql_query(
                SQQUERY_TS.format(tag), trs, index_col='id'))
            self.data.execute(TABLE.format(tag))
            features.reset_index(inplace=True)
            features = features.fillna(0.0)     # 填补缺失值
            features.to_sql(tag, self.data, if_exists='append', index=False)


if __name__ == "__main__":
    DG = DatasetGenerator()
    OPT = input("""
        Choose Mode(1-2):
        [1] Generate raw data.
        [2] Download complete data.
    """)
    if OPT == '1':
        DG.preprocessing()
    elif OPT == '2':
        DG.dataset_gen()
