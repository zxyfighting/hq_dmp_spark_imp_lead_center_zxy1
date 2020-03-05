package com.faw.hq.dmp.spark.imp.thread.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.faw.hq.dmp.spark.imp.thread.realize.LeadKafkaConsumer.LeadCenterNew

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object JdbcUtil {
  def findCount(conn: Connection,prepareStmt: PreparedStatement,vfrom1:String ,vfrom2:String,vfrom3:String,vfrom4:String,region_code:String,province_code:String ): Int = {
    var count: Integer = 0;
    var sql = "select  count(0) as count from ads_lead_count_daily where vfrom1 = '"+vfrom1+"' and vfrom2 = '"+vfrom2+"' and vfrom3 = '"+vfrom3+"' and vfrom4 = '"+vfrom4+"' and region_code = '"+region_code+"' and province_code = '"+province_code+"'" ;
      var prepareStmt = conn.prepareStatement(sql)
      val rs = prepareStmt.executeQuery(sql)
      while (rs != null && rs.next()) {
        count = rs.getInt("count")
      }
      if (prepareStmt != null) {
        prepareStmt.close();
      }
      if (rs != null) {
        rs.close();
      }
     count
  }





  def insertChange(conn: Connection,prepareStmt: PreparedStatement,dateString: String,leadCenter: LeadCenterNew): Unit ={
    try {

      var sql = "insert into ads_lead_count_daily (lz_date,vfrom1,vfrom2,vfrom3,vfrom4,region_code,province_code,lead_cnt,create_time,update_time) values(?,?,?,?,?,?,?,?,now(),now())";
      var prepareStmt = conn.prepareStatement(sql);
      prepareStmt.setString(1, dateString)
      prepareStmt.setString(2, leadCenter.vfrom1)
      prepareStmt.setString(3, leadCenter.vfrom2)
      prepareStmt.setString(4, leadCenter.vfrom3)
      prepareStmt.setString(5, leadCenter.vfrom4)
      prepareStmt.setString(6, leadCenter.regionCode)
      prepareStmt.setString(7,leadCenter.provinceCode)
      prepareStmt.setLong(8, leadCenter.preLeadCount)
     prepareStmt.execute()
    }
    finally {
      if (prepareStmt != null) {
        prepareStmt.close();
      }
    }
  }

  def updateChange(conn: Connection,prepareStmt: PreparedStatement,preLeadCount:Long,vfrom1:String ,vfrom2:String,vfrom3:String,vfrom4:String,region_code:String,province_code:String): Unit ={
    try {
      var sql = "update ads_lead_count_daily set  lead_cnt='"+preLeadCount+"' ,update_time=now() where  vfrom1 = '"+vfrom1+"' and vfrom2 = '"+vfrom2+"' and vfrom3 = '"+vfrom3+"' and vfrom4 = '"+vfrom4+"' and region_code = '"+region_code+"' and province_code = '"+province_code+"'";
      var prepareStmt = conn.prepareStatement(sql);
      prepareStmt.execute()
    }
    finally {
      if (prepareStmt != null) {
        prepareStmt.close();
      }
    }
  }



}