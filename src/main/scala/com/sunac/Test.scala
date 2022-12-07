package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    spark.sql(SqlConfig.ES_CHARGE_OWNER_FEE_ALL).createTempView("all_tab")
    val join1 =
      """
        |select
        | t1.*,
        | t2.fld_name,
        | t2.fld_dq,
        | t2.fld_ywdy,
        | t2.fld_xm,
        | t2.fld_company,
        | t2.fld_confirm_date,
        | t2.fld_fee_type,
        | t2.fld_yt,
        | t2.fld_guid as a_fld_guid
        |  from all_tab t1 left join test.es_info_area_info t2
        |on t1.fld_area_guid=t2.fld_guid
        |""".stripMargin
    spark.sql(join1).createTempView("t12")
    val join2 =
      """
        |select
        | t12.*,
        | case when t12.fld_reason_guid='' then ''
        | else t3.fld_name end as fld_reason_name
        | from t12 left join test.es_info_param_info t3
        | on t12.fld_reason_guid=t3.fld_guid
        |""".stripMargin
    spark.sql(join2).createTempView("t123")
    val join3 =
      """
        |select
        | t.*,
        | t4.fld_name as fld_object_name,
        | t4.fld_owner_fee_date,
        | t4.fld_batch,
        | t4.fld_building,
        | t4.fld_cell,
        | t4.fld_charged_area,
        | t4.fld_status as fld_obj_status,
        | t4.fld_start_fee_date,
        | t4.fld_guid as o_fld_guid,
        | t4.fld_order as obj_fld_order
        | from t123 t left join test.es_info_object t4
        |on t.fld_object_guid=t4.fld_guid
        |""".stripMargin
    spark.sql(join3).repartitionByRange(200, col("fld_guid"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang1")
    spark.stop()
  }
}
