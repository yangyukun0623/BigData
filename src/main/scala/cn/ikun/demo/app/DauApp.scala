package cn.ikun.demo.app

import java.util.Date

import cn.hutool.core.date.DateUtil
import cn.ikun.demo.tools.{JedisTemplate, KafkaTool, PropertiesUtil}
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.sparkDStreamFunctions
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

//@Component
class DauApp extends Serializable {

    def run() = {
        //创建配置对象
        val conf = new SparkConf().setAppName("GMALL_REALTIME_DAU").setMaster("local[2]")
        conf.set("es.index.auto.create", "true")
        //创建StreamingContext对象
        val ssc = new StreamingContext(conf,Seconds(5))
        val dstream = KafkaTool.createDStream(ssc,PropertiesUtil.get("kafka.dau.topic"))
        val result = dstream.mapPartitions(records => {
            records.foreach(record => {
                val jedis = new JedisTemplate
                val json = JSON.parseObject(record.value())
                val ts = json.getLong("ts")
                val mid = json.getJSONObject("common").getString("mid")
                val date = new Date(ts)
                val dd = DateUtil.format(date,"yyyy-MM-dd")
                val dh = DateUtil.format(date,"yyyy-MM-dd:HH")
                jedis.opsForSet.add("dau:" + dh, mid, 24 * 60 * 60 * 2 + 60 * 60)
                jedis.opsForSet.add("dau:" + dd, mid, 24 * 60 * 60 * 2 + 60 * 60)
            })
            records
        })
        result.print()
        val es = Map[String, String]("es.nodes" -> "localhost:9200")
        sparkDStreamFunctions(result).saveToEs("/dau_list_info/_doc", es)

        //启动
        ssc.start()
        ssc.awaitTermination()
    }
}

object DauApp {
    def main(args: Array[String]): Unit = {
        val app = new DauApp
        app.run()
    }
}




























































