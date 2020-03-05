package com.faw.hq.dmp.spark.imp.thread.util
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.fasterxml.jackson.databind.ObjectMapper
import com.faw.hq.dmp.spark.imp.thread.bean.{OneIdUnity, ThreadBean}

/**
  * author:ZhangXiuYun
  * purpose：存放方法
  */
object GetMethods {

  /**
    * json解析方法
    *
    */
  def getLead(json:String): String = {
    val objectMapper = new ObjectMapper
    val appInfo = objectMapper.readValue(json, classOf[ThreadBean])
    appInfo.jsonToLine()
  }

  def getOneIdUtil(createTime:String): OneIdUnity ={
    val oneIdUnity = new OneIdUnity
    oneIdUnity.setOneId(null)
    oneIdUnity.setIdCard(null)
    oneIdUnity.setDriverNo(null);
    oneIdUnity.setaId(null);
    oneIdUnity.setMobile(null);
    oneIdUnity.setCookieId(null);
    oneIdUnity.setDeviceId(null);
    oneIdUnity.setUnionId(null);
    oneIdUnity.setOpenId(null);
    oneIdUnity.setVin(null);
    oneIdUnity.setFaceId(null);
    oneIdUnity.setSource(null);
    oneIdUnity.setCreateTime(new SimpleDateFormat( "yyyy-MM-dd").parse(createTime));
    oneIdUnity.setUpdateTime(null)
    oneIdUnity
  }
  def  getSecondsNextEarlyMorning() ={
    var cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, 1)
    // 改成这样就好了
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    (cal.getTimeInMillis() - System.currentTimeMillis())
  }


  def main(args: Array[String]): Unit = {
    val json5 = "\"{\"\"pkId\"\": 467, \"\"vfrom1\"\": \"\"OFF\"\", \"\"vfrom2\"\": \"\"F2\"\", \"\"vfrom3\"\": \"\"F22\"\", \"\"vfrom4\"\": \"\"F2202\"\", \"\"cityCode\"\": \"\"04\"\", \"\"cityName\"\": \"\"?挎.甯.\", \"\"dealerId\"\": \"\"DJL606\"\", \"\"newLeadHQ\"\": \"\"\"\", \"\"preLeadId\"\": \"\"XSZXN2019112100028DJL606\"\", \"\"countyCode\"\": \"\"40\"\", \"\"createTime\"\": \"\"2019-11-21 20:27:00.000000\"\", \"\"dealerName\"\": \"\"?..?.?宸.孩?.苯杞?.?..?℃.?..??\", \"\"regionCode\"\": null, \"\"regionName\"\": null, \"\"seriesCode\"\": \"\"H5\"\", \"\"seriesName\"\": null, \"\"updateTime\"\": null, \"\"countryName\"\": \"\"楂..?\u0080?.骇涓.??..\"\", \"\"provinceCode\"\": \"\"JL\"\", \"\"provinceName\"\": \"\"?..?.\", \"\"newLeadDealer\"\": \"\"true\"\"}\""
    val json6 = "\"{\"\"pkId\"\": 464, \"\"vfrom1\"\": \"\"OFF\"\", \"\"vfrom2\"\": \"\"F1\"\", \"\"vfrom3\"\": \"\"F11\"\", \"\"vfrom4\"\": \"\"F1103\"\", \"\"cityCode\"\": \"\"04\"\", \"\"cityName\"\": \"\"?挎.甯.\", \"\"dealerId\"\": \"\"DJL605\"\", \"\"newLeadHQ\"\": \"\"\"\", \"\"preLeadId\"\": \"\"XSZXN2019112100025DJL605\"\", \"\"countyCode\"\": \"\"39\"\", \"\"createTime\"\": \"\"2019-11-21 13:52:02.000000\"\", \"\"dealerName\"\": \"\"?挎.?.北绾㈡.姹借溅?\u0080?..?℃.?..??\", \"\"regionCode\"\": \"\"HB\"\", \"\"regionName\"\": \"\"?..\"\", \"\"seriesCode\"\": \"\"HS7\"\", \"\"seriesName\"\": null, \"\"updateTime\"\": null, \"\"countryName\"\": \"\"姹借溅缁.??\u0080?.??..\"\", \"\"provinceCode\"\": \"\"JL\"\", \"\"provinceName\"\": \"\"?..?.\", \"\"newLeadDealer\"\": \"\"true\"\"}\""
    val json4 = json6.replace("\"{\"","{").replace("\"}\"","}").replace("\"\"","\"")
    val json7="{\"pkId\":136805,\"preLeadId\":\"XSZXN2019112100028DJL606\",\"dealerName\":\"吉林省神州红旗汽车销售服务有限公司\",\"seriesCode\":\"H5\",\"dealerId\":\"DJL606\",\"cityCode\":\"04\",\"provinceCode\":\"JL\",\"seriesName\":\"\",\"regionName\":\"\",\"updateTime\":\"\",\"countyCode\":\"40\",\"regionCode\":\"\",\"newLeadDealer\":\"\",\"cityName\":\"长春市\",\"newLeadHQ\":\"\",\"createTime\":\"2019-11-21 20:27:00\",\"vfrom4\":\"F2202\",\"id\":136805,\"countryName\":\"高新技术产业开发区\",\"provinceName\":\"吉林省\",\"vfrom2\":\"F2\",\"vfrom3\":\"F22\",\"vfrom1\":\"OFF\"}"
    val jaon1="{\"pkId\":136806,\"preLeadId\":\"XSZXN2019112400001qweqwe123\",\"dealerName\":\"\",\"seriesCode\":\"\",\"dealerId\":\"qweqwe123\",\"cityCode\":\"\",\"provinceCode\":\"\",\"seriesName\":\"\",\"regionName\":\"\",\"updateTime\":\"\",\"countyCode\":\"\",\"regionCode\":\"\",\"newLeadDealer\":\"true\",\"cityName\":\"\",\"newLeadHQ\":\"\",\"createTime\":\"2019-11-24 13:39:42\",\"vfrom4\":\"N3202\",\"id\":136806,\"countryName\":\"\",\"provinceName\":\"\",\"vfrom2\":\"N3\",\"vfrom3\":\"N32\",\"vfrom1\":\"ON\"}"
    val json8="{\"pkId\":467,\"preLeadId\":\"XSZXN2019112100028DJL606\",\"dealerName\":\"?..?.?宸.孩?.苯杞?.?..?℃.?..??,\"seriesCode\":\"H5\",\"dealerId\":\"DJL606\",\"cityCode\":\"04\",\"provinceCode\":\"JL\",\"seriesName\":\"\",\"regionName\":\"\",\"updateTime\":\"2019-11-27 09:17:14\",\"countyCode\":\"40\",\"regionCode\":\"\",\"newLeadDealer\":\"true\",\"cityName\":\"?挎.甯.,\"newLeadHQ\":\"\",\"createTime\":\"2019-11-21 20:27:00\",\"vfrom4\":\"F2202\",\"id\":467,\"countryName\":\"楂..?\u0080?.骇涓.??..\",\"provinceName\":\"?..?.,\"vfrom2\":\"F2\",\"vfrom3\":\"F22\",\"vfrom1\":\"OFF\"}"
    val json0 = "{\"pkId\":455,\"preLeadId\":\"XSZXN2019112100016DJL605\",\"dealerName\":\"?挎.?.北绾㈡.姹借溅?\u0080?..?℃.?..??,\"seriesCode\":\"H5\",\"dealerId\":\"DJL605\",\"cityCode\":\"04\",\"provinceCode\":\"JL\",\"seriesName\":\"H5\",\"regionName\":\"?.腑\",\"updateTime\":\"2019-11-27 09:17:14\",\"countyCode\":\"39\",\"regionCode\":\"HZ\",\"newLeadDealer\":\"true\",\"cityName\":\"?挎.甯.,\"newLeadHQ\":\"\",\"createTime\":\"2019-11-21 12:36:00\",\"vfrom4\":\"N1501\",\"id\":455,\"countryName\":\"姹借溅缁.??\u0080?.??..\",\"provinceName\":\"?..?.,\"vfrom2\":\"N1\",\"vfrom3\":\"N15\",\"vfrom1\":\"ON\"}"
    val jsonn= "{\"pkId\":22569595,\"preLeadId\":\"XSZXN2019112907687EAH603\",\"dealerName\":\"合肥大步牛星汽车销售服务有限公司\",\"seriesCode\":\"H5\",\"dealerId\":\"EAH603\",\"cityCode\":\"06\",\"provinceCode\":\"AH\",\"seriesName\":\"\",\"regionName\":\"\",\"updateTime\":\"2019-11-29 19:48:33\",\"countyCode\":\"04\",\"regionCode\":\"\",\"newLeadDealer\":\"false\",\"cityName\":\"合肥市\",\"newLeadHQ\":\"\",\"createTime\":\"2019-11-29 19:48:35\",\"vfrom4\":\"N1301\",\"id\":22569595,\"countryName\":\"包河区\",\"provinceName\":\"安徽省\",\"vfrom2\":\"N1\",\"vfrom3\":\"N13\",\"vfrom1\":\"ON\"}"
    //println(json4)
    //println(json4)
    val objectMapper = new ObjectMapper
    val appInfo = objectMapper.readValue(jsonn, classOf[ThreadBean])
    println(appInfo.jsonToLine())

  }

}

