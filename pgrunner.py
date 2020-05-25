# pgrunner.py
# 动态更新pagerank值
# python 3.x

from datetime import date, timedelta
import sqlite3
import json
from tqdm import tqdm
import networkx as nx
from google.oauth2 import service_account
import pandas_gbq

# 10个热门标签
TAGS = "SELECT DISTINCT tag FROM `{:s}.SOFeature.Score600`"

# 获取数据集内的用户ID
USER = """
    SELECT owner_user_id AS user_id,
    DATE_ADD(DATE(creation_date), INTERVAL {period} DAY) AS judge_date,
    0.0 AS pagerank
    FROM `{proj}.SOFeature.Score600`
    WHERE tag='{tag}'
"""

# 获取一周的回答排名数据
QUES = """
    SELECT src, tar, COUNT(*) weight
    FROM `{proj}.SOFeature.QuestionRankS`,
        UNNEST(rank_id) AS src WITH OFFSET off_s,
        UNNEST(rank_id) AS tar WITH OFFSET off_t
    WHERE DATE(creation_date) BETWEEN '{days}' AND '{daye}'
        AND '{tag}' IN UNNEST(tags)
        AND rank[OFFSET(off_t)]-rank[OFFSET(off_s)]<0
    GROUP BY src, tar
"""

# 标签特殊字符转义
def tra(tag):
    return tag.replace('#', '_sharp').replace('++', '_plus')

# PageRank排名更新
class PGRunner():
    # 初始化
    def __init__(self):
        with open('auth.json', 'r') as auth_file:
            cred = json.load(auth_file)
            self.cred = service_account.Credentials.from_service_account_info(cred)
            self.proj = cred['project_id']
        self.prdb = sqlite3.connect("Data/PageRank.sqlite")
        self.oriday = date(2008, 7, 31)     # SO的创建日期
        self.pager = {}
        tqdm.write("-" * 50 + "\nPageRank Runner")

    # 析构函数
    def __del__(self):
        self.prdb.close()

    # 析构函数
    def _pager(self, uid):
        if uid in self.pager:
            return self.pager[uid]
        else:
            return 0.0

    # 执行BigQuery查询
    def _query(self, query):
        return pandas_gbq.read_gbq(query, credentials=self.cred, progress_bar_type=None)

    # 建立空的数据表
    def create_table(self):
        tags = self._query(TAGS.format(self.proj))
        for tag in list(tags['tag']):
            self.prdb.execute(
                """CREATE TABLE IF NOT EXISTS {:s} (
                        user_id INTEGER PRIMARY KEY NOT NULL,
                        judge_date DATE NOT NULL,
                        pagerank REAL NOT NULL);
                """.format(tra(tag)))   # 建立数据表的格式
            self.prdb.execute("DELETE FROM {:s};".format(tra(tag)))
            self.prdb.execute("DROP INDEX IF EXISTS idx_{:s};".format(tra(tag)))
            # 执行BigQuery查询，获取数据表
            udata = self._query(USER.format(**{'tag': tag, 'period': 120, 'proj':self.proj}))
            udata.to_sql(tra(tag), self.prdb, if_exists='append', index=False)
            self.prdb.execute(  # 建立索引
                "CREATE INDEX idx_{tag} ON {tag} (judge_date);".format(**{'tag': tra(tag)}))
            self.prdb.commit()
            tqdm.write("User table {:s} created.".format(tra(tag)))

    # 动态更新
    def run(self, tag):
        # 初始化日期迭代器
        tqdm.write("-" * 50 + "\nStart ranking...")
        cnt = (date.today() - self.oriday).days-629+120
        graph = nx.DiGraph()           # 创建空有向图
        # 每周迭代一次
        for ind in tqdm(range(int(cnt / 7)), leave=False):
            day_s = (self.oriday + timedelta(ind*7)).isoformat()    # 起始日期
            day_e = (self.oriday + timedelta(ind*7+6)).isoformat()  # 终止日期
            args = {'tag': tag, 'proj':self.proj, 'days': day_s, 'daye': day_e}
            # 获取按照排名生成的边
            edges = [tuple(row) for _, row in self._query(QUES.format(**args)).iterrows()]
            # 将边加入有向图中
            for (src, tar, weight) in edges:
                if graph.has_edge(src, tar):
                    graph[src][tar]['weight'] += weight        # 添加新边
                else:
                    graph.add_edge(src, tar, weight=weight)    # 增加已有边的权重
            pr_start = dict.fromkeys(
                graph, 1.0 / graph.number_of_nodes())
            pr_start.update(self.pager)
            try:    # PageRank更新
                self.pager = nx.pagerank_scipy(graph, max_iter=200, nstart=pr_start)
            except nx.PowerIterationFailedConvergence:
                pass
            # 对本周到达时间窗口末尾的用户，提取并归一化其PageRank值，存储至表中
            judge_user = self.prdb.execute(
                """
                    SELECT user_id FROM {:s}
                    WHERE judge_date BETWEEN '{:s}' AND '{:s}'
                """.format(tra(tag), day_s, day_e)).fetchall()
            self.prdb.executemany("UPDATE {:s} SET pagerank=? WHERE user_id=?".format(
                tra(tag)), [[self._pager(id)*graph.number_of_nodes(), id] for (id,) in judge_user])
            tqdm.write("Week: {:s}-{:s} updated.".format(day_s, day_e))

        self.prdb.commit()
        tqdm.write("-" * 50 + "\n{:s}: PageRank ranking finished.".format(tag))

    # 动态更新所有话题标签
    def runall(self):
        tags = self._query(TAGS.format(self.proj))
        for tag in tqdm(list(tags['tag'])):
            self.run(tag)


if __name__ == "__main__":
    PGR = PGRunner()
    PGR.create_table()
    PGR.runall()
