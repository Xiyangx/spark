package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * @author litten
 * @date 2022/6/23 15:30
 */
object Test2 {
  def JdbcUtil(spark: SparkSession, sql: String) = {
    spark
      .read
      .option("fetchsize", 5000)
      .format("jdbc")
      .jdbc(SqlConfig.JDBC_STR, sql, SqlConfig.prop)
  }

  //
  def JdbcUtilBySlide2(spark: SparkSession, sqlExecute: String, slideSql: String, slideCol: String) = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val frame: DataFrame = spark
      .read
      .format("jdbc")
      .option("fetchsize", 5000)
      .jdbc(SqlConfig.JDBC_STR, slideSql, SqlConfig.prop)

    var pres: Array[String] = null
    if (slideCol.equals("fld_start_date")) {
      pres = frame.collect().map(t1 => "date_format( fld_start_date, '%Y-%m' )='" + t1.getString(0) + "'")
    } else {
      pres = frame.collect().map(t1 => slideCol + "='" + t1.getString(0) + "'")
    }
    spark
      .read
      .format("jdbc")
      .option("fetchsize", 5000)
      .jdbc(SqlConfig.JDBC_STR, sqlExecute, pres, SqlConfig.prop)
  }

  def JdbcUtilBySlide(spark: SparkSession, sqlExecute: String, tabName: String) = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    spark
      .read
      .format("jdbc")
      .option("batchsize", 5000)
      .jdbc(SqlConfig.JDBC_STR, sqlExecute, SqlConfig.getBestArray(spark, tabName), SqlConfig.prop)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val join5 =
      """
        |select
        | t.*,
        | t2.fld_name as fld_owner_name,
        | t2.fld_guid as w_fld_guid,
        | t2.fld_phone_number,
        | t2.fld_desc as fld_owner_desc
        |   from  test.xiyang1 t left join test.es_info_owner t2
        |on t.fld_owner_guid=t2.fld_guid
        |""".stripMargin
    spark.sql(join5).createTempView("t5")

    val join6 =
      """
        |select
        | t5.*,
        | t2.fld_name as fld_project_name,
        | t2.fld_guid as p_fld_guid,
        | t2.fld_object_type
        | from t5 left join test.es_charge_project t2
        |on t5.fld_project_guid=t2.fld_guid
        |""".stripMargin
    spark.sql(join6).createTempView("t6")

    val join7 =
      """
        |select
        | f.*,
        | t2.fld_guid as ecrr_fld_guid,
        | t2.fld_general_tax as fld_rate,
        | t2.fld_general_tax as ecrr_fld_general_tax,
        |(f.fld_amount / (1+if(if(t2.fld_general_tax=null,-1,t2.fld_general_tax)=-1 or t2.fld_general_tax=-2, 0 ,t2.fld_general_tax))) * if(if(t2.fld_general_tax=null,-1,t2.fld_general_tax)=-1 or t2.fld_general_tax=-2, 0 ,t2.fld_general_tax) as fld_taxes
        | from t6 f left join test.es_charge_rate_result t2
        | on t2.fld_area_guid = f.fld_area_guid
        | and t2.fld_project_guid = f.fld_project_guid
        | and f.fld_finance_date <= t2.fld_end_date
        | and f.fld_finance_date >= t2.fld_start_date
        |
        |""".stripMargin
    spark.sql(join7).createTempView("t7")


    val join8 =
      """
        |select
        | f.*,
        | t8.fld_guid as c_fld_guid,
        | t8.fld_name as fld_object_class_name
        | from t7 f left join test.es_info_object_class t8
        | on f.o_fld_guid=t8.fld_guid
        |""".stripMargin
    spark.sql(join8).createTempView("t8ok")

    spark.sql(join8)
      .repartitionByRange(200, col("fld_guid"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang2")
    spark.stop()
  }
}
