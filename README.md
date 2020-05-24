# StackOverflow潜在专家预测 数据获取

## 数据介绍

本部分为数据获取模块，使用StackOverflow在BigQuery存储的公开数据库获取数据。请确保使用本模块时用户端或服务器**能正常访问Google**。

## 数据获取流程

0. 进行Google Cloud认证文件。
1. 运行**dataset.py**，选择‘1’进行预处理。（约2分钟）
2. 运行**pagerank.py**，进行PageRank动态更新。（约12小时，确保运行期间网络畅通）
3. 运行**trueskill.py**，进行TrueSkill动态更新。（约12小时，确保运行期间网络畅通）
4. 运行**dataset.py**，选择‘2’进行数据获取，获取的数据存储在**Data/StackExpert.sqlite**（约15分钟）

## Google Cloud 认证方法
1. 使用Google账户登录[Google Cloud](https://console.cloud.google.com/)，会自动创建默认的Project。
2. 在Google Cloud Console进入[BigQuery](https://console.cloud.google.com/bigquery),选择默认的Project，创建数据集‘SOFeature’。
3. 进入[IAM和管理-服务账号管理](https://console.cloud.google.com/iam-admin/serviceaccounts)，选择默认的Project，点击‘创建新的服务账号’，设置名称后，在“向此服务帐号授予对项目的访问权限”中选择**BigQuery Admin**,在“向用户授予访问此服务帐号的权限”中选择生成密钥，下载json格式认证文件，完成服务账号设置。
4. 将认证文件更名为‘auth.json’，替换本目录中的认证文件。