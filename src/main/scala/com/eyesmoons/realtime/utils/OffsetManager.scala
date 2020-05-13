package com.eyesmoons.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManager {
    val offsetPre: String = PropertiesUtil.getProperty("redis.offset.pre")

    /**
     * 从redis中获取偏移量
     * @param groupId
     * @param topic
     * @return
     */
    def getOffset(groupId:String,topic:String): Map[TopicPartition, Long] = {
        val jedis: Jedis = RedisUtil.getJedisClient

        val redisOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetPre + groupId + ":" + topic)

        val set: util.Set[util.Map.Entry[String, String]] = redisOffsetMap.entrySet()

        val iter: util.Iterator[util.Map.Entry[String, String]] = set.iterator()

        val map: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
        while (iter.hasNext) {
            val next: util.Map.Entry[String, String] = iter.next()

            val partition: String = next.getKey
            val offset: String = next.getValue

            val topicPartition = new TopicPartition(topic, partition.toInt)
            map.put(topicPartition,offset.toLong)
        }

        jedis.close()
        map.toMap
    }

    /**
     * 保存offset到redis
     * @param groupId
     * @param topic
     */
    def saveOffset(groupId: String, topic: String,offsetRanges: Array[OffsetRange]) = {
        if (offsetRanges != null && offsetRanges.size > 0) {
            val jedis: Jedis = RedisUtil.getJedisClient
            val key: String = offsetPre + groupId + ":" + topic
            val value: util.HashMap[String, String] = new util.HashMap[String, String]()

            for (elem <- offsetRanges) {
                value.put(elem.partition.toString,elem.untilOffset.toString)
            }
            jedis.hmset(key,value)
            jedis.close()
        }
    }
}
