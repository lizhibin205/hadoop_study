在执行前，必须把数据导入hdfs

# 数据格式

```
[year] [temperature]
年份4位 温度2位，最大温度为40
```

# 执行

```shell
hadoop jar mapReduceDemo1.jar com.bytrees.bigdata.MaxTemperature [input] [output]
```

或者

```shell
export $HADOOP_CLASSPATH=mapReduceDemo1.jar
hadoop com.bytrees.bigdata.MaxTemperature [input] [output]
```
