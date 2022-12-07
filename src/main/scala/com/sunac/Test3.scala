package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))


    val s =
      """
        |select
        |t1.*,
        |t2.fld_period_guid as pj_fld_period_guid,
        |t2.fld_guid as pj_fld_guid,
        |t2.fld_project_guid as pj_fld_project_guid
        | from test.xiyang3 t1 left join test.es_charge_project_period_join t2
        | on t1.fld_project_guid=t2.fld_project_guid
        |""".stripMargin

    spark.sql(s).createTempView("pj")

    val s2 =
      """
        |select
        |t.*,
        |t2.fld_guid as pp_fld_guid,
        |t2.fld_name as fld_project_period_name
        |from pj t left join test.es_charge_project_period t2
        |on t.pj_fld_period_guid=t2.fld_guid
        |and t2.fld_type=1
        |""".stripMargin
    spark.sql(s2).createTempView("ppOk")

    val s3 =
      """
        |select fld_guid,fld_area_guid,fld_status,fld_owner_fee_guid,fld_adjust_guid,fld_adjust_guid as fld_settle_adjust_guid,fld_main_guid
        |from(
        |select *, row_number()over(partition by fld_area_guid,fld_owner_fee_guid,fld_adjust_guid order by fld_create_date desc) ranking
        |from test.es_charge_settle_accounts_detail
        |) t where t.ranking = 1;
        |""".stripMargin


    spark.sql(s3).createTempView("ad")

    val s4 =
      """
        |select
        |t.*,
        |t1.fld_guid as ad_fld_guid,
        |t1.fld_main_guid as ad_fld_main_guid,
        |t1.fld_adjust_guid as fld_settle_adjust_guid,
        |t1.fld_owner_fee_guid
        |from ppOk t
        |left join ad t1
        |on ( t1.fld_owner_fee_guid = t.fld_guid OR t1.fld_adjust_guid = t.fld_guid )
        |	AND t1.fld_status = 1
        |	AND t1.fld_area_guid = t.fld_area_guid
        |""".stripMargin
    spark.sql(s4).createTempView("adOk")
    val s5 =
      """
        |select
        |t.*,
        |t1.fld_guid as am_fld_guid,
        |t1.fld_settle_status,
        |t1.fld_attribute,
        |t1.fld_bill_no as fld_settle_bill_no,
        |t1.fld_guid as fld_main_guid,
        |t1.fld_examine_status,
        |t1.fld_examine_date
        |from adOk t
        |left join test.es_charge_settle_accounts_main t1
        |ON t.ad_fld_main_guid = t1.fld_guid
        |AND t1.fld_area_guid = t.fld_area_guid
        |AND t1.fld_examine_status = 4
        |""".stripMargin
    spark.sql(s5).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang4")
    spark.stop()
  }
}
