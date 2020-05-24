--   Score.sql
--   分数数据生成
--   SQL(BigQuery)

-- 01 生成用户创建600天内的回答序列
CREATE OR REPLACE TEMP TABLE usr_ans AS(
    SELECT a.id,
        TIMESTAMP_DIFF(a.creation_date, u.creation_date, DAY) span,
        u.creation_date
    FROM `bigquery-public-data.stackoverflow.posts_answers` a
    JOIN `bigquery-public-data.stackoverflow.users` u ON u.id=a.owner_user_id
    WHERE community_owned_date IS NULL AND
        TIMESTAMP_DIFF(a.creation_date, u.creation_date, DAY) BETWEEN 0 AND 600
        AND u.creation_date <= '{udate}'
);
-- 02 统计用户创建600天内的回答的分数
CREATE OR REPLACE TEMP TABLE ans_score AS(
    SELECT ua.id,
        ANY_VALUE(ua.span) span,
        ANY_VALUE(ua.creation_date) creation_date,
        COUNTIF(v.vote_type_id=2)-COUNTIF(v.vote_type_id=3) score600,
        COUNTIF(v.vote_type_id=2 AND TIMESTAMP_DIFF(v.creation_date, ua.creation_date, DAY) BETWEEN 0 AND 120)-COUNTIF(v.vote_type_id=3 AND TIMESTAMP_DIFF(v.creation_date, ua.creation_date, DAY) BETWEEN 0 AND 120) score120,
        SUM(IF(v.vote_type_id=1, 1, 0)) acception,
    FROM `bigquery-public-data.stackoverflow.votes` v
    JOIN usr_ans ua ON v.post_id=ua.id
    AND TIMESTAMP_DIFF(v.creation_date, ua.creation_date, DAY) BETWEEN 0 AND 600
    AND v.vote_type_id BETWEEN 1 AND 3
    GROUP BY ua.id
);
-- 03 生成回答分数数据表
CREATE OR REPLACE TABLE SOFeature.AnswerScore
    OPTIONS (
    description="Answers score."
    ) AS
    SELECT ua.id,
        ua.span,
        IFNULL(sc.score120,0) AS score,
        IFNULL(sc.acception, 0) AS acception
    FROM ans_score sc
    RIGHT JOIN usr_ans ua ON sc.id=ua.id
    WHERE ua.span BETWEEN 0 AND {period}
;
-- 04 创建用户600天内获得分数表
CREATE OR REPLACE TABLE SOFeature.Score600
OPTIONS (
 description="Questions score rank and acceptions. Partitioned by date."
) AS
-- 05 获取问题包含的话题标签
WITH ques_tag AS(
  SELECT
    id,
    SPLIT(tags, '|') tags,
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE community_owned_date IS NULL
),
-- 06 过滤出前10的热门话题标签 
ques_contain AS(
  SELECT
    id,
    tag_n tag
  FROM ques_tag, ques_tag.tags AS tag_n
  JOIN (SELECT tag_name FROM `bigquery-public-data.stackoverflow.tags` ORDER BY count DESC LIMIT 10) ht
    ON ht.tag_name=tag_n
)
-- 07 生成话题标签[tag]下用户600天内获得的分数
SELECT a.owner_user_id,
  qc.tag,
  ANY_VALUE(sc.creation_date) creation_date,
  SUM(sc.score600) score600
FROM ques_contain qc
JOIN `bigquery-public-data.stackoverflow.posts_answers` a ON a.parent_id=qc.id
JOIN ans_score sc ON sc.id=a.id
GROUP BY a.owner_user_id, qc.tag
;
-- 08 创建回答质量统计表
CREATE OR REPLACE TABLE SOFeature.ContentQuality
OPTIONS (
 description="Answer content quality, including entropy, word count, body length."
) AS
-- 09 生成词频统计序列
WITH body_array AS(
  SELECT pa.id,
    SPLIT(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REPLACE(
                  LOWER(body),      -- 大小写转换
                '\n', ' '),         -- 清除换行符
              '<(b|h)r>', ' '       -- 清除<br><hr>单个标签
            ),'<(kb|co|su|pr|a|bl).*?>.*?</(kb|co|su|pr|a|bl).*?>', ' '), -- 清除非文本标签及其中包含的内容
          '<[/]?[^><]+>',' '),      -- 清除剩余所有HTML标签
        '[,.!?:;"\'\\(\\)&]',' '),  -- 清除标点符号
      ' {2,}', ' '),                -- 合并超过1个空格
    ' ') AS word_array              -- 以空格为分隔符进行分割
  FROM `bigquery-public-data.stackoverflow.posts_answers` pa
  JOIN `bigquery-public-data.stackoverflow.users` u 
    ON u.id=pa.owner_user_id
  WHERE community_owned_date IS NULL 
    AND TIMESTAMP_DIFF(pa.creation_date, u.creation_date, DAY) BETWEEN 0 AND {period}
    AND u.creation_date <= '{udate}'
)
--10 计算内容熵、单词数、内容长度
SELECT id,
  SUM(- freq * log(freq, 2)) entropy,
  SUM(cnt) word_cnt,
  COUNT(cnt) body_len
FROM(
  SELECT id,
    COUNT(word) / SUM(COUNT(word)) OVER(PARTITION BY id) freq,
    COUNT(word) cnt
  FROM body_array, UNNEST(body_array.word_array) AS word
  WHERE LENGTH(word)>0 AND NOT REGEXP_CONTAINS(word, '[^a-z]')
  GROUP BY id, word
)
GROUP BY id;
--11 创建话题标签相似度表
CREATE OR REPLACE TABLE SOFeature.TagRelation
OPTIONS (
 description="Tag similarity."
) AS
WITH source_tag AS(     -- 提取提问数10000以上的标签
  SELECT tag_name, count FROM `bigquery-public-data.stackoverflow.tags` WHERE count>10000)
,target_tag AS(         -- 提取提问数前10的标签
  SELECT tag_name, count FROM source_tag ORDER BY count DESC LIMIT 10)
,tag_array AS(
  SELECT id, SPLIT(tags, '|') tags FROM `bigquery-public-data.stackoverflow.posts_questions`)
,source_array AS(
  SELECT id, ARRAY(SELECT tag FROM UNNEST(tags) AS tag JOIN source_tag ON tag_name=tag) src FROM tag_array)
,target_array AS(
  SELECT id, ARRAY(SELECT tag FROM UNNEST(tags) AS tag JOIN target_tag ON tag_name=tag) tar FROM tag_array)
,relation_count AS(
  SELECT src, tar, COUNT(*) AS weight
  FROM(SELECT * FROM source_array JOIN target_array USING(id)), UNNEST(src) AS src, UNNEST(tar) AS tar
  GROUP BY src, tar)
SELECT src AS source, tar AS target, ROUND(weight/count,3) AS weight FROM relation_count JOIN source_tag ON tag_name=src
WHERE weight/count>0.2; -- 保留相似度大于0.2的相关标签记录
--12 删除临时表，处理完成
DROP TABLE ans_score;
DROP TABLE usr_ans;
SELECT 'Success';
