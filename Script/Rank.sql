--   Rank.sql
--   回答排名数据生成
--   SQL(BigQuery)

-- 01 创建回答排名表格
CREATE OR REPLACE TABLE SOFeature.QuestionRankS
PARTITION BY DATE(creation_date)
OPTIONS (
 description="Questions score rank and acceptions. Partitioned by date."
) AS
-- 02 统计时间窗口内的回答获得的分数
WITH scoretb AS (
 SELECT post_id id,
  COUNTIF(vote_type_id=2) - COUNTIF(vote_type_id=3) scores
  FROM `bigquery-public-data.stackoverflow.votes` v
  JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON a.id=v.post_id
  JOIN `bigquery-public-data.stackoverflow.posts_questions` q ON a.parent_id=q.id
  WHERE TIMESTAMP_DIFF(v.creation_date, q.creation_date, DAY) BETWEEN 0 AND {period} AND
    vote_type_id BETWEEN 2 AND 3
  GROUP BY post_id
),
-- 03 计算同一问题下答案按照分数获得的排名
ans_rank AS(
  SELECT  
    parent_id,
    owner_user_id,
    accepted_answer_id,
    DENSE_RANK() OVER(PARTITION BY parent_id ORDER BY IFNULL(scoretb.scores, 0) DESC, DATE(pa.creation_date) ASC) rank
  FROM `bigquery-public-data.stackoverflow.posts_answers` pa
  JOIN `bigquery-public-data.stackoverflow.users` u ON u.id=owner_user_id
  LEFT JOIN scoretb ON scoretb.id=pa.id
  WHERE pa.creation_date BETWEEN u.creation_date AND '{adate}'
),
-- 04 获取问题包含的话题标签
ques_tag AS(
  SELECT
    id,
    SPLIT(tags, '|') tags,
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE community_owned_date IS NULL
    AND answer_count>0
    AND creation_date<='{adate}'
),
-- 05 过滤出前10的热门话题标签
ques_contain AS(
  SELECT
    id,
    ARRAY_AGG(tag_n) tag
  FROM ques_tag, ques_tag.tags AS tag_n
  JOIN (SELECT tag_name FROM `bigquery-public-data.stackoverflow.tags` ORDER BY count DESC LIMIT 10) ht
    ON ht.tag_name=tag_n
  GROUP BY id
),
-- 06 生成每个问题对应的回答排名序列
ques_rank AS(
SELECT
  id,
  ANY_VALUE(q.creation_date) creation_date,
  ANY_VALUE(tag) tags,
  ARRAY_AGG(ar.owner_user_id ORDER BY rank) rank_id,
  ARRAY_AGG(rank ORDER BY rank) rank,
  IFNULL(ANY_VALUE(q.owner_user_id),0) asker_id,
  ANY_VALUE(q.accepted_answer_id) accepted_answer_id
FROM ques_contain qc
JOIN ans_rank ar ON qc.id=ar.parent_id
JOIN `bigquery-public-data.stackoverflow.posts_questions` q USING(id)
GROUP BY id
HAVING COUNT(*)>0)
-- 07 加入答案获得采纳的情况
SELECT 
  qr.* EXCEPT (accepted_answer_id),
  pa.owner_user_id accept_id
FROM ques_rank qr
LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` pa
    ON qr.accepted_answer_id=pa.id
    AND NOT pa.owner_user_id=qr.asker_id;
-- 08 完成数据整理
SELECT 'Success';