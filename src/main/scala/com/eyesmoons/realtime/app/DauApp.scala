package com.eyesmoons.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.eyesmoons.realtime.beans.DauInfo
import com.eyesmoons.realtime.utils.{MyEsUtil, MyKafkaUtil, OffsetManager, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauApp {
    def main(args: Array[String]): Unit = {
        val groupId: String = PropertiesUtil.getProperty("kafka.group_id")
        val topic: String = PropertiesUtil.getProperty("kafka.topic")

        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

        val ssc = new StreamingContext(conf, Seconds(3))

        //val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        //从自定义偏移量读取kafka数据
        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic)
        val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap,groupId)

        //获取数据的offsetRanges
        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
        val transformDs: DStream[ConsumerRecord[String, String]] = ds.transform {
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        //startJsonDs.print(100)

        val startJsonDs: DStream[JSONObject] = transformDs.map {
            record => {
                JSON.parseObject(record.value())
            }
        }

        //经过redis去重
        val distinctDs: DStream[JSONObject] = startJsonDs.mapPartitions {
            record => {
                val startLogList: List[JSONObject] = record.toList
                val jedis: Jedis = RedisUtil.getJedisClient
                val list = new ListBuffer[JSONObject]

                println("过滤前大小：" + startLogList.size)
                for (bean <- startLogList) {
                    val ts: Long = bean.getLong("ts")
                    val mid: String = bean.getJSONObject("common").getString("mid")
                    val tsStr: String = sdf.format(new Date(ts))
                    val ifFirst: lang.Long = jedis.sadd("dau:" + tsStr, mid)
                    if (ifFirst == 1L) {
                        list += bean
                    }
                }
                println("过滤后大小：" + list.size)
                jedis.close()
                list.toIterator
            }
        }
        //distinctDs.print(100)

        //将jsonObject封装成对象
        val dauInfoDs: DStream[DauInfo] = distinctDs.map {
            record => {
                val commJson: JSONObject = record.getJSONObject("common")
                val mid: String = commJson.getString("mid")
                val uid: String = commJson.getString("uid")
                val ar: String = commJson.getString("ar")
                val ch: String = commJson.getString("ch")
                val vc: String = commJson.getString("vc")
                val ts: Long = record.getLong("ts")

                val tsStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(ts))
                val tsArr: Array[String] = tsStr.split(" ")
                val dt: String = tsArr(0)
                val timeArr: Array[String] = tsArr(1).split(":")
                val hr: String = timeArr(0)
                val mi: String = timeArr(1)

                DauInfo(mid, uid, ar, ch, vc, dt, hr, mi, ts)
            }
        }

        //保存到ES中
        dauInfoDs.foreachRDD{rdd=>{
            rdd.foreachPartition{
                iter=>{
                    if(offsetRanges != null && offsetRanges.size > 0) {
                        for (elem <- offsetRanges) {
                            println("偏移量：" + elem.fromOffset + "-->" + elem.untilOffset)
                        }
                    }

                    val tuples: List[(String, DauInfo)] = iter.toList.map {
                        dauInfo => (dauInfo.mid, dauInfo)
                    }
                    val dt: String = sdf.format(new Date())
                    MyEsUtil.bulkSave(tuples,"gmall_dau_info" + dt)
                }
                OffsetManager.saveOffset(groupId,topic,offsetRanges)
            }
        }}

        ssc.start()
        ssc.awaitTermination()
    }
}
