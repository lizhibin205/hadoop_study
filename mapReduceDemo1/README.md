在执行前，必须把数据导入hdfs

# 执行

```shell
hadoop jar mapReduceDemo1.jar com.bytrees.bigdata.MaxTemperture [input] [output]
```

或者

```shell
export $HADOOP_CLASSPATH=mapReduceDemo1.jar
hadoop com.bytrees.bigdata.MaxTemperture [input] [output]
```
