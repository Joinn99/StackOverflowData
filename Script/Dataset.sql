--   Dataset.sql
--   数据集生成
--   SQL(BigQuery)

-- 01 创建与话题标签[TAG]相关的问题集合
CREATE TEMP TABLE Q_LIST AS
    WITH Q_ARY AS(
        SELECT id,
        owner_user_id,
        creation_date,
        SPLIT(tags, '|') tag_array
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE community_owned_date IS NULL AND NOT
        (answer_count=0 AND comment_count=0)
    )
    SELECT id,
        ANY_VALUE(owner_user_id) user_id,
        ANY_VALUE(creation_date) creation_date,
        MAX(TR.weight) weight
    FROM Q_ARY, Q_ARY.tag_array AS tag
    JOIN SOFeature.TagRelation TR
    ON TR.target='{tag}' AND TR.source=tag AND TR.weight>0.4 -- 主题相似度>0.4
    GROUP BY id
;
-- 02 创建与话题标签[TAG]相关的回答活动序列
CREATE TEMP TABLE A_ACT AS
    SELECT ASL.id,
        PA.owner_user_id,
        ASL.score score,
        ASL.acception,
        IFNULL(CQ.entropy,0) cont_entropy,
        IFNULL(CQ.word_cnt,1) cont_cnt,
        IFNULL(CQ.body_len,1) cont_len,
        ASL.span span,
        Q_LIST.weight
    FROM SOFeature.AnswerScore ASL
    LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` PA ON PA.id=ASL.id
    LEFT JOIN SOFeature.ContentQuality CQ ON PA.id=CQ.id
    JOIN Q_LIST ON PA.parent_id=Q_LIST.id
    JOIN SOFeature.Score600 SC
        ON SC.owner_user_id=PA.owner_user_id AND SC.tag='{tag}'
;
-- 03 删除与话题标签[TAG]相关的问题集合
DROP TABLE Q_LIST;
-- 04 生成用户话题标签[TAG]下的回答特征
CREATE TEMP TABLE A_FEATURE AS
    SELECT owner_user_id,
        COUNT(*) CNT_A, 
        AVG(score) AVG_A_SCORE, MAX(score) MAX_A_SCORE, SUM(score) SUM_A_SCORE, APPROX_QUANTILES(score, 2)[OFFSET(1)] MED_A_SCORE, 
        AVG(span) AVG_A_SPAN, MAX(span) MAX_A_SPAN, SUM(span) SUM_A_SPAN, APPROX_QUANTILES(span, 2)[OFFSET(1)] MED_A_SPAN, 
        AVG(cont_entropy) AVG_A_ENTRO, MAX(cont_entropy) MAX_A_ENTRO, SUM(cont_entropy) SUM_A_ENTRO, APPROX_QUANTILES(cont_entropy, 2)[OFFSET(1)] MED_A_ENTRO, 
        AVG(cont_cnt) AVG_A_CNT, MAX(cont_cnt) MAX_A_CNT, SUM(cont_cnt) SUM_A_CNT, APPROX_QUANTILES(cont_cnt, 2)[OFFSET(1)] MED_A_CNT, 
        AVG(cont_len) AVG_A_LEN, MAX(cont_len) MAX_A_LEN, SUM(cont_len) SUM_A_LEN, APPROX_QUANTILES(cont_len, 2)[OFFSET(1)] MED_A_LEN
    FROM A_ACT
    WHERE weight=1.0 -- 只提取包含标签[TAG]问题下的回答
    GROUP BY owner_user_id
    HAVING COUNT(*)>2 -- 选取回答数至少为3的用户
;
-- 05 生成用户与话题标签[TAG]相关的回答特征
CREATE TEMP TABLE T_FEATURE AS
    SELECT owner_user_id,
        SUM(weight) CNT_T,
        SUM(weight*score) SUM_T_SCORE, MAX(weight*score) MAX_T_SCORE,
        SUM(weight*span) SUM_T_SPAN, MAX(weight*span) MAX_T_SPAN
    FROM A_ACT
    GROUP BY owner_user_id
;
-- 06 删除与话题标签[TAG]相关的回答活动序列
DROP TABLE A_ACT;
-- 07 生成用户的提问特征
CREATE TEMP TABLE Q_FEATURE AS
    SELECT Q.owner_user_id,
        COUNT(*) CNT_Q,
        AVG(TIMESTAMP_DIFF(Q.creation_date, SC.creation_date, DAY)) SUM_Q_SPAN,
        MAX(TIMESTAMP_DIFF(Q.creation_date, SC.creation_date, DAY)) MAX_Q_SPAN
    FROM `bigquery-public-data.stackoverflow.posts_questions` Q
    JOIN SOFeature.Score600 SC
        ON SC.owner_user_id=Q.owner_user_id AND SC.tag='{tag}'
        AND TIMESTAMP_DIFF(Q.creation_date, SC.creation_date, DAY) BETWEEN 0 AND {period}
    GROUP BY Q.owner_user_id
;
-- 08 生成用户的评论特征
CREATE TEMP TABLE C_FEATURE AS
    SELECT C.user_id owner_user_id,
        COUNT(*) CNT_C,
        AVG(TIMESTAMP_DIFF(C.creation_date, SC.creation_date, DAY)) SUM_C_SPAN,
        MAX(TIMESTAMP_DIFF(C.creation_date, SC.creation_date, DAY)) MAX_C_SPAN
    FROM `bigquery-public-data.stackoverflow.comments` C
    JOIN `regal-muse-268709.SOFeature.Score600` SC
        ON SC.owner_user_id=C.user_id AND SC.tag='{tag}'
        AND TIMESTAMP_DIFF(C.creation_date, SC.creation_date, DAY) BETWEEN 0 AND {period}
    GROUP BY C.user_id
;
-- 09 整合除问答排名以外的所有特征
SELECT owner_user_id id,
    SC.score600 EXPERT_SCORE,   -- 数据集标签，EXPERT_SCORE>=100即为专家
    A_FEATURE.* EXCEPT(owner_user_id),
    T_FEATURE.* EXCEPT(owner_user_id),
    Q_FEATURE.* EXCEPT(owner_user_id),
    C_FEATURE.* EXCEPT(owner_user_id)
FROM SOFeature.Score600 SC
JOIN A_FEATURE USING(owner_user_id)
LEFT JOIN T_FEATURE USING(owner_user_id)
LEFT JOIN Q_FEATURE USING(owner_user_id)
LEFT JOIN C_FEATURE USING(owner_user_id)
WHERE SC.tag='{tag}'
;