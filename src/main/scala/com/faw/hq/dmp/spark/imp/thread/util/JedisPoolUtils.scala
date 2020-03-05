package com.faw.hq.dmp.spark.imp.thread.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

case class RedisConfig(redisHost: String, redisPort: Int, redisTimeout: Int, redisPass:String,maxTotal: Int,
                       maxIdle: Int, minIdle: Int) extends  Serializable

object JedisPoolUtils extends Serializable {

  @transient private var pool: JedisPool = null

  def makePool(redisConfig:RedisConfig): Unit ={
    makePool(redisConfig.redisHost, redisConfig.redisPort, redisConfig.redisTimeout,redisConfig.redisPass, redisConfig.maxTotal,
      redisConfig.maxIdle, redisConfig.minIdle)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, redisPass:String,maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
    makePool(redisHost, redisPort, redisTimeout, redisPass,maxTotal, maxIdle, minIdle, true, false, 10000)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, redisPass:String,maxTotal: Int, maxIdle: Int, minIdle: Int,
               testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if (pool == null) {
      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout,redisPass)

      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }


}
