##Flink的处理时间
https://cloud.tencent.com/developer/article/1378586

     ProcessingTime, 以operator处理的时间为准，使用的是机器的系统时间来作为data stream的时间
     IntestionTime,  以数据进入flink streaming data flow的时间为准
     EventTime;   以数据自带的时间戳字段为准，应用程序需要制定如何从record中抽取时间戳字段