# tsrunner.py
# 动态更新trueskill值
# python 3.x

from datetime import date, timedelta
import sqlite3
import json
from tqdm import tqdm
from google.oauth2 import service_account
import pandas_gbq
import trueskill

# 10个热门标签
TAGS = "SELECT DISTINCT tag FROM `{:s}.SOFeature.Score600`"

# 获取数据集内的用户ID
USER = """
    SELECT DISTINCT user_id,
        DATE_ADD(DATE(SC.creation_date), INTERVAL {period} DAY) AS judge_date,
        0.0 AS ts,
        25.0 AS mu,
        25.0 / 3 AS sigma
    FROM `{proj}.SOFeature.QuestionRankS`,
        `{proj}.SOFeature.QuestionRankS`.rank_id AS user_id
    LEFT JOIN `{proj}.SOFeature.Score600` SC
        ON SC.owner_user_id = user_id AND SC.tag = '{tag}'
    WHERE '{tag}' IN UNNEST(tags)
"""

# 获取一周的回答排名数据
QUES = """
    SELECT tag,
        rank_id,
        rank,
        asker_id,
        accept_id
    FROM `{proj}.SOFeature.QuestionRankS`,
         `{proj}.SOFeature.QuestionRankS`.tags AS tag
    WHERE DATE(creation_date) BETWEEN '{dates}' AND '{datee}'
"""

# 标签特殊字符转义
def tra(tag):
    return tag.replace('#', '_sharp').replace('++', '_plus')

# 提取TrueSkill值 (mu-3*sigma)
def tse(group):
    return (group[0].mu, group[0].sigma)

# TrueSkill排名更新
class TrueSkillRunner():
    # 初始化
    def __init__(self):
        with open('auth.json', 'r') as auth_file:
            cred = json.load(auth_file)
            self.cred = service_account.Credentials.from_service_account_info(cred)
            self.proj = cred['project_id']
        self.tsdb = sqlite3.connect("Data/TrueSkill.sqlite")
        self.tsrate = trueskill.TrueSkill(
            backend='mpmath', draw_probability=0.2)
        self.oriday = date(2008, 7, 31)
        self.fresh = trueskill.Rating(8.3333, 2.7778)
        self.tags = self._query(TAGS.format(self.proj))
        tqdm.write("-" * 50 + "\nTrueSkill Runner")

    # 析构函数
    def __del__(self):
        self.tsdb.commit()
        self.tsdb.close()

    # 执行BigQuery查询
    def _query(self, query):
        return pandas_gbq.read_gbq(query, credentials=self.cred, progress_bar_type=None)

    # 建立空的数据表
    def create_table(self):
        tqdm.write("-" * 50 + "\nStart creating tables...")
        for tag in tqdm(self.tags):
            # 执行BigQuery查询，获取数据表
            users = self._query(USER.format(**{'tag': tag, 'period': 120, 'proj':self.proj}))
            self.tsdb.execute(
                """CREATE TABLE IF NOT EXISTS {:s} (
                    user_id INTEGER PRIMARY KEY NOT NULL,
                    judge_date DATE,
                    ts REAL NOT NULL,
                    mu REAL NOT NULL,
                    sigma REAL NOT NULL);
                """.format(tra(tag)))    # 建立数据表的格式
            users.to_sql(tra(tag), self.tsdb, if_exists='append', index=False)
            self.tsdb.execute(  # 建立索引
                "CREATE INDEX idx_{tag} ON {tag} (judge_date);".format(**{'tag': tra(tag)}))
            tqdm.write('Creation: Tag {:s} database created.'.format(tra(tag)))
        self.tsdb.commit()
        tqdm.write('Database all created.')

    # 动态更新
    def run(self):
        # 初始化日期迭代器
        tqdm.write("-" * 50 + "\nStart ranking...")
        cnt = (date.today() - self.oriday).days-629+120
        # 每周迭代一次
        for ind in tqdm(range(int(cnt / 7))):
            day_s = (self.oriday + timedelta(ind*7)).isoformat()    # 起始日期
            day_e = (self.oriday + timedelta(ind*7+6)).isoformat()  # 终止日期
            # 获取回答排名数据
            ques = self._query(QUES.format(**{'dates': day_s, 'datee': day_e, 'proj':self.proj}))
            for _, row in tqdm(ques.iterrows(), total=ques.shape[0], leave=False):
                tag = tra(row['tag'])
                cur = self.tsdb.execute(    # 获取本周有回答记录的用户ID
                    "SELECT user_id, mu, sigma FROM {:s} WHERE user_id IN {}".format(
                        tag, tuple(row['rank_id'])+(row['asker_id'], )))
                user_ts = {id: (mu, sigma)  # 提取本周有回答记录的用户mu与sigma值
                           for (id, mu, sigma) in cur.fetchall()}
                if row['accept_id'] in user_ts:     # 使用提问者-被采纳者的关系更新
                    rate_gp = [(trueskill.Rating(user_ts[row['accept_id']]), )]
                    if row['asker_id'] in user_ts:         # 有记录的用户，创建TS的比赛对象
                        rate_gp.append(
                            (trueskill.Rating(user_ts[row['asker_id']]), ))
                        rate_gp = self.tsrate.rate(rate_gp)
                    else:                                  # 对于没有记录的用户，使用默认值
                        rate_gp.append((self.fresh, ))
                        rate_gp = self.tsrate.rate(rate_gp)
                        user_ts.update({row['asker_id']: tse(rate_gp[1])})
                    user_ts.update({row['accept_id']: (tse(rate_gp[0]))})
                if len(row['rank_id']) > 1:         # 使用回答者-回答者的关系更新
                    # 创建TS的比赛对象
                    rate_gp = [(trueskill.Rating(user_ts[id]),)
                               for id in row['rank_id']]
                    # 更新mu与sigma值
                    rate_gp = self.tsrate.rate(rate_gp, ranks=row['rank'])
                    user_ts.update({id: tse(ts)
                                    for ts, id in zip(rate_gp, row['rank_id'])})
                # 保存更新后的mu与sigma值
                self.tsdb.executemany("UPDATE {:s} SET mu=?, sigma=? WHERE user_id=?".format(tag),
                                      [ts+(id, ) for (id, ts) in user_ts.items()])
            # 对本周到达时间窗口末尾的用户，提取TrueSkill值，存储至表中
            for tag in list(self.tags['tag']):
                self.tsdb.execute(
                    """UPDATE {:s} SET ts=mu-3*sigma
                    WHERE judge_date BETWEEN '{:s}' AND '{:s}'""".format(tra(tag), day_s, day_e))
            self.tsdb.commit()
            tqdm.write("Date: {:s}-{:s} | Update finished.".format(day_s, day_e))
        tqdm.write("-" * 50 + "\nTrueSkill ranking finished.")


if __name__ == "__main__":
    TSR = TrueSkillRunner()
    TSR.create_table()
    TSR.run()
