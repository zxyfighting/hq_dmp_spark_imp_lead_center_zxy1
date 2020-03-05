package com.faw.hq.dmp.spark.imp.thread.realize

import java.{lang, util}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.faw.hq.dmp.spark.imp.thread.bean.Result
import com.faw.hq.dmp.spark.imp.thread.util._
import com.mysql.jdbc.{Connection, PreparedStatement, StringUtils}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Pipeline

/**
  *
  * auth:张秀云
  * purpose:线索中心指标：总线索量；是红旗内线索的统计；是本店内的新线索的统计
  */
object LeadKafkaConsumer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    while (true) {
      //配置spark上下文环境对象
      val processInterval =59
      val conf = new SparkConf().setAppName("lead_center").setMaster("local[*]")
      val sc = new SparkContext(conf)
      //设置采集器每十秒批次拉取数据
      val sparkstream = new StreamingContext(sc, Seconds(processInterval))

      /**
        * 参数获取
        */
      // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
      val module: String = "Test1"
      val config: FileBasedConfiguration = ConfigUtil("config.properties").config
      val chkDir = config.getString("hdfs.chk")
      sparkstream.sparkContext.setCheckpointDir(chkDir)
      val hdfs_url = config.getString("hdfs.url")
      val brokerList = config.getString("kafka.broker.list")
      val topic = config.getString("kafka.topic")
      val topics = Array(topic)
      //消费者组的名称
      val groupid = config.getString("group.id")
      //获取mysql配置
      val url = config.getString("jdbc.url")
      val userName = config.getString("jdbc.user")
      val password = config.getString("jdbc.password")
      // sparkstreaming 消费 kafka 时的 Consumer 参数
      val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> groupid,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
      )

      // 初始化 Redis 连接池
      JedisPoolUtils.makePool(RedisConfig("prod.dbaas.private", 16359, 30000, "realtime123", 1000, 100, 50))
      val kafkaStream = KafkaRedisUtils.createDirectStream(sparkstream, kafkaParams, module, groupid, topics)
      //开始处理批次消息
      kafkaStream.foreachRDD(rdd => {
        //获取当前批次的RDD的偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 处理从获取 kafka 中的数据
        if (!rdd.isEmpty()) {
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          //开启事务
          val pipeline: Pipeline = jedisClient.pipelined()
          pipeline.multi()

          try {

            //更新offset到Redis中
            offsetRanges.foreach({ offsetRange =>
              logger.info("==========> partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset
                + " untilOffset: " + offsetRange.untilOffset)

              // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
              val key = s"${module}:${groupid}:${offsetRange.topic}"
              pipeline.hset(key, offsetRange.partition.toString, offsetRange.untilOffset.toString)
            })
            //提交事务
            pipeline.exec()
            //关闭pipeline
            pipeline.sync()
          } catch {
            case e: Exception => {
              logger.error("数据处理异常", e)
              pipeline.discard()
            }
          } finally {
            //关闭连接
            pipeline.close()
            jedisClient.close()
          }
        }
      })
      //将一条条json数据解
      val resultDestream1: DStream[String] = kafkaStream.map(_.value()).map(json => {
        val info: String = GetMethods.getLead(json)
        info
      })
      resultDestream1.cache()
      //将数据上传到hdfs上
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dateString: String = dateFormat.format(new Date())
      val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateString1: String = dateFormat1.format(new Date())
      resultDestream1.foreachRDD(rdd => {
        val rdd1 = rdd.map((_, " "))
        val currentTime: Date = new Date()
        //val formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        val formatter = new SimpleDateFormat("yyyy-MM-dd/");
        val dateString = formatter.format(currentTime);
        RDD.rddToPairRDDFunctions(rdd1).partitionBy(new HashPartitioner(1)).
          saveAsHadoopFile(hdfs_url + "dt=" + dateString, classOf[String],
            classOf[String], classOf[AppendTextOutputFormat])
      })

      //获取连接oneid地址，将数据推送到oneId平台
      val oneId = config.getString("oneId.ip")
      resultDestream1.foreachRDD(rdd => {
        rdd.foreach(rdd => {
          val strings = rdd.split(";")
          try{
            val oneIdUnity = GetMethods.getOneIdUtil(strings(1))
            val url = oneId
            val str: String = JSON.toJSONString(oneIdUnity, SerializerFeature.PrettyFormat)
            val result = HttpClientExtendUtil.doPostRequestBody(url, str)
            val json: Result = JSON.parseObject(result, classOf[Result])
            println(json)
          }
          catch{
            case ex:Exception => logger.error("捕获异常")
          }
        })
      })

      //求总线索量
      val leadIdCounts1: DStream[(String, Long)] = resultDestream1.map(rdd => (dateString, 1)).updateStateByKey {
        case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
          val sumCount = seq.sum + buffer.getOrElse(0L)
          Option(sumCount) //表往缓存里边更新对值　　它需要返回一个Option
        }
      }
      leadIdCounts1.foreachRDD(cs => {

        var conn: Connection = null;
        var ps: PreparedStatement = null;
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          cs.foreachPartition(f => {
            var conn = DriverManager.getConnection(url, userName, password);
            var ps = conn.prepareStatement("replace into dmp_behavior_clue_amounts(clue_dt,clue_amounts,create_time,update_time) values(?,?,?,now())");
            f.foreach(s => {
              ps.setString(1, s._1);
              ps.setLong(2, s._2);
              ps.setString(3, dateString1);
              ps.executeUpdate();
            })
          })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }
      })

      //求是红旗内线索的统计
      val newLeadHQCounts1: DStream[(String, Long)] = resultDestream1.filter(rdd => {
        val strings = rdd.split(";")
        strings(18).equals("true")
      }).map(rdd => (dateString, 1)).updateStateByKey {
        case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
          val sumCount = seq.sum + buffer.getOrElse(0L)
          Option(sumCount) //表往缓存里边更新对值　　它需要返回一个Option
        }
      }
      newLeadHQCounts1.foreachRDD(cs => {
        var conn: Connection = null;
        var ps: PreparedStatement = null;
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          cs.foreachPartition(f => {
            var conn = DriverManager.getConnection(url, userName, password);
            var ps = conn.prepareStatement("replace into dmp_behavior_clue_hq_amounts(clue_hq_dt,clue_hq_amounts,create_time,update_time) values(?,?,?,now())");
            f.foreach(s => {
              ps.setString(1, s._1);
              ps.setLong(2, s._2);
              ps.setString(3, dateString1);
              ps.executeUpdate();
            })
          })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }
      })

      val timeout = config.getString("redis.expireTime")
      val leadCenterDestream: DStream[(LeadKey,Long)] = resultDestream1.
        filter(rdd=>{
          val strings = rdd.split(";")
          !strings(19).equals("\\N") && !strings(14).equals("\\N") && !strings(15).equals("\\N") && !strings(16).equals("\\N")&& !strings(17).equals("\\N")&& !strings(10).equals("\\N") && !strings(8).equals("\\N") && strings(19).equals("true")
        }).
        map(rdd => {
        val strings = rdd.split(";")
        val leadCenter = LeadCenter(dateString, strings(14), strings(15), strings(16), strings(17), strings(10), strings(8),strings(19).toBoolean, dateString1, dateString1)
        val key = LeadKey(dateString, strings(14), strings(15), strings(16), strings(17), strings(10), strings(8))
        (key, 1L)
      })

      val leadKey: DStream[(LeadKey, Long)] = leadCenterDestream.reduceByKey(_+_)
      leadKey.foreachRDD(rdd=>{
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
        rdd.foreachPartition(rdd=>{
          var conn = DriverManager.getConnection(url, userName, password);
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
             rdd.foreach(elem=>{
               var prekey = elem._1.lzDate+"_"+elem._1.vfrom1+"_"+elem._1.vfrom2+"_"+elem._1.vfrom3+"_"+elem._1.vfrom4 +"_"+elem._1.regionCode+"_"+elem._1.provinceCode+"_"+ dateString
               jedisClient.expire(prekey,timeout.toInt * 60 * 24)
               var preLeadCount:Long = jedisClient.hincrBy(prekey,"prelead",elem._2)
               /*if (elem.newLeadDealer.==(true))
               {
                 var preLeadCount:Long = jedisClient.hincrBy(prekey,"prelead",1L)
               }
               else if(jedisClient.hexists(prekey,"prelead")){
                 var preLeadCount: Long = jedisClient.hget(prekey,"prelead").toLong
               }
               else {
                 var preLeadCount:Long = jedisClient.hset(prekey,"prelead","1")
               }
               var preLeadCount = jedisClient.hget(prekey,"prelead").toLong*/

               var center = LeadCenterNew(dateString, elem._1.vfrom1, elem._1.vfrom2, elem._1.vfrom3, elem._1.vfrom4, elem._1.regionCode,
                 elem._1.provinceCode,preLeadCount)
               var count = JdbcUtil.findCount(conn,ps,elem._1.vfrom1,elem._1.vfrom2,elem._1.vfrom3,elem._1.vfrom4,elem._1.regionCode,elem._1.provinceCode)
               if(count>0)
               {
                 JdbcUtil.updateChange(conn, ps,preLeadCount,elem._1.vfrom1,elem._1.vfrom2,elem._1.vfrom3,elem._1.vfrom4,elem._1.regionCode,elem._1.provinceCode)
               }
               else
                 {
                   JdbcUtil.insertChange(conn,ps,dateString,center)
               }

          })
          jedisClient.close()
        })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }
        })

     /*leadKey.foreachRDD(rdd=>{
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
        rdd.foreachPartition(rdd=>{
          var conn = DriverManager.getConnection(url, userName, password);
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          rdd.foreach(rdd=>{
             rdd._2.foreach(elem=>{
               var keylead = "lead"+elem.lzDate+elem.vfrom1+elem.vfrom2+elem.vfrom3+elem.vfrom4 +elem.regionCode+elem.provinceCode +dateString
               var prekey = "prelead" +elem.lzDate+elem.vfrom1+elem.vfrom2+elem.vfrom3+elem.vfrom4 +elem.regionCode+elem.provinceCode+ dateString
               jedisClient.expire(keylead,timeout.toInt * 60 * 24)
               jedisClient.expire(prekey,timeout.toInt * 60 * 24)
               var leadCount: Long = jedisClient.hincrBy(keylead,"lead",1L)
               if (elem.newLeadDealer.==(true))
               {
                 var preLeadCount:Long = jedisClient.hincrBy(prekey,"prelead",1L)
               }
               else if(jedisClient.hexists(prekey,"prelead")){
                 var preLeadCount: Long = jedisClient.hget(prekey,"prelead").toLong
               }
               else {
                 var preLeadCount:Long = jedisClient.hset(prekey,"prelead","1")
               }
               var preLeadCount = jedisClient.hget(prekey,"prelead").toLong

               var center = LeadCenterNew(dateString, elem.vfrom1, elem.vfrom2, elem.vfrom3, elem.vfrom4, elem.regionCode,
                 elem.provinceCode,elem.newLeadDealer, leadCount,preLeadCount,dateString1, dateString1)
               var count = JdbcUtil.findCount(conn,ps,elem.vfrom1,elem.vfrom2,elem.vfrom3,elem.vfrom4,elem.regionCode,elem.provinceCode)
               if(count>0)
               {
                 JdbcUtil.updateChange(conn, ps,leadCount,preLeadCount,elem.vfrom1,elem.vfrom2,elem.vfrom3,elem.vfrom4,elem.regionCode,elem.provinceCode)
               }
               else
                 {
                   JdbcUtil.insertChange(conn,ps,dateString,center)
               }

             })

          })
          jedisClient.close()
        })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }
        })*/

      //求是本店内的新线索的统计
      val newLeadDealerCounts1: DStream[(String, Long)] = resultDestream1.filter(rdd => {
        val strings = rdd.split(";")
        strings(19).equals("true")
      }).map(rdd => (dateString, 1)).updateStateByKey {
        case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
          val sumCount1 = seq.sum + buffer.getOrElse(0L)
          Option(sumCount1) //表往缓存里边更新对值　　它需要返回一个Option
        }
      }
      newLeadDealerCounts1.foreachRDD(cs => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          cs.foreachPartition(f => {
            var conn = DriverManager.getConnection(url, userName, password);
            var ps = conn.prepareStatement("replace into dmp_behavior_clue_new_amounts(clue_new_dt,clue_new_amounts,create_time,update_time) values(?,?,?,now())");
            f.foreach(s => {
              ps.setString(1, s._1);
              ps.setLong(2, s._2);
              ps.setString(3, dateString1);
              ps.executeUpdate();
            })
          })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close();
          }
        }

      })

      //开启采集器
      sparkstream.start()
      sparkstream.awaitTerminationOrTimeout(GetMethods.getSecondsNextEarlyMorning())
      sparkstream.stop(false, true)


    }
  }
    case class ThreadInfo(leadId: String, createTime: String, dealerId: String, dealerName: String, countyCode: String, countyName: String,
                          cityCode: String, cityName: String, provinceCode: String, provinceName: String, regionCode: String, regionName: String,
                          seriesCode: String, seriesName: String, vfrom1: String, vfrom2: String, vfrom3: String, vfrom4: String, newLeadHQ: Boolean, newLeadDealer: Boolean)

  case class LeadCenterNew(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String,preLeadCount:Long)
  case class LeadCenter(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String,newLeadDealer:Boolean,createTime:String,updateTime:String)
   case class LeadKey(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String)
}