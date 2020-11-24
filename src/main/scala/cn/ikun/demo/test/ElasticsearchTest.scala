package cn.ikun.demo.test

import cn.ikun.demo.tools.{KafkaTool, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.sparkStringJsonDStreamFunctions

object ElasticsearchTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingEs")
            .set("spark.es.nodes", "localhost")
            .set("spark.es.port", "9200")
            .set("es.index.auto.create", "true")
        //创建StreamingContext对象
        val ssc = new StreamingContext(conf,Seconds(1))
        val dstream = KafkaTool.createDStream(ssc,PropertiesUtil.get("kafka.dau.topic"))
        val result = dstream.map(record => record.value())
            .map(str => "{\"test\": \"" + str + "\"}")
        result.print()
        //写入Elasticsearch
        sparkStringJsonDStreamFunctions(result).saveJsonToEs("/spark/doc")
        ssc.start()
        ssc.awaitTermination()
    }
}
