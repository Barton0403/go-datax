# Go Datax
基于[Datax](https://github.com/alibaba/DataX)封装的数据库同步工具，支持生成job文件、批量执行job文件、server/client交互模式

# System Requirements
+ Linux
+ [JDK 1.8+](https://www.python.org/downloads/)
+ [Golang 1.18+](https://go.dev/dl/)

# Quick Start
## 下载[Datax](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202209/datax.tar.gz)
下载[Datax](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202209/datax.tar.gz)解压到到根目录
## 配置
config/app.yaml
```
source_db:
  database: source # 源数据库名
  username: root
  password: root
  host: 127.0.0.1
  port: 3306

target_db:
  database: target # 目标数据库名
  username: root
  password: root
  host: 127.0.0.1
  port: 3306
```
## 生成Job文件
```
go run . generate
```
## 同步数据
```
go run . run all
```