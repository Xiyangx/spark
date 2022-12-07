package com.sunac

import com.sunac.Test2.{JdbcUtil, JdbcUtilBySlide2}
import org.apache.spark.sql.SparkSession

object TableMoveTask2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    if (args(0) == "1"){
      val Slide_OBJ_Sql: String = SqlConfig.SLIDE_SQL.format("fld_area_guid", "es_info_object", "fld_area_guid")
      SqlConfig.insertHiveUtil(JdbcUtilBySlide2(spark, SqlConfig.ES_INFO_OBJECT, Slide_OBJ_Sql, "fld_area_guid"), "xiyang_es_info_object")
    }
    if (args(0) == "2"){
      val Slide_COF_Sql: String = SqlConfig.SLIDE_SQL.format("fld_area_guid", "es_charge_owner_fee", "fld_area_guid")
      SqlConfig.insertHiveUtil(JdbcUtilBySlide2(spark, SqlConfig.ES_CHARGE_OWNER_FEE, Slide_COF_Sql, "fld_area_guid"), "xiyang_es_charge_owner_fee")
    }
    spark.stop()
  }
}
