
## 项目说明
基于 `spark` 对 `movielens` 的数据分析和推荐模型，数据分析部分使用 `pyspark` , 推荐模型使用 `spark` 的 `mlib` 库中 `ALS` 模型。

## 结构说明                       
- `code`
  - `analysis.py` 为数据分析代码，
  - `JavaALSExample.java `为推荐模型的主要代码，
  - 完整项目为 `als-project`
  - `als.jar` 是推荐模型的打包文件。
- `data`：`movielens` 的数据集，里面 README 有详细的数据说明
- `output`：推荐模型的输出结果

## 环境配置
```
虚拟机 : VMware Workstation Pro 14    
Ubuntu 镜像 : ubuntu-18.04-live-server-amd64.ios    
jdk  : 1.8.0_161  
节点 : master slave1 slave2    
hadoop: 2.8.4   
spark: 2.3.1
python: 3.6
```
## 运行项目
将打包好的 `als.jar` 传到虚拟机中
启动 `hadoop`
启动 `spark`
将数据集上传到 `HDFS` 中，同时确保输出文件的目录不存在，否则会报错
运行 `spark-submit` 命令，提交 `jar` 包

## 数据来源
http://grouplens.org/datasets/movielens/1m/ 
