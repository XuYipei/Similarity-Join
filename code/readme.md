*Scalable_All-Pairs_Similarity_Search_in_Metric_Spaces*

输入文件：/pointrepart/input/test.txt

运行方式：

```shell
hadoop fs -rm -r /pointrepart/output/*
/usr/hadoop/bin/hadoop jar ~/folder/pointrepart.jar pointrepart 10 0.0002 /dataset/middle /pointrepart/output/sample /pointrepart/output/cluster /pointrepart/output/repart /pointrepart/output/similarity /pointrepart/output/datain
```

结果在 /pointrepart/output/similarity 下

聚类距离阈值 $100$，随机采样概率为 $0.01$



*ClusterJoin_A_Similarity_Joins_Framework_using_MapReduce*

输入文件：/clusterjoin/input/test.txt

运行方式：

```shell
hadoop fs -rm -r /clusterjoin/output/*
/usr/hadoop/bin/hadoop jar ~/folder/clusterjoin.jar clusterjoin 10 0.0002 /dataset/middle /clusterjoin/output/sample /clusterjoin/output/cluster /clusterjoin/output/similarity /clusterjoin/output/datain
```

结果在 /clusterjoin/output/similarity 下

聚类距离阈值 $100$，随机采样概率为 $0.01$







