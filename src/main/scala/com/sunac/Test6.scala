package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test6 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val s6 =
      """
        |select fld_guid,fld_owner_fee_guid,fld_incoming_fee_guid,fld_create_date,fld_operate_date,fld_create_user,fld_busi_type,fld_total,fld_amount,fld_late_fee,fld_tax_amount,fld_tax,fld_cancel,fld_late_amount
        |from(
        |select *, row_number()over(partition by fld_owner_fee_guid order by fld_operate_date desc) ranking
        |from test.es_charge_incoming_data
        |) t where t.ranking = 1;
        |""".stripMargin

    spark.sql(s6).createTempView("da")
    val s7 =
      """
        |select
        |  t.*,
        |  t2.fld_guid as da_fld_guid,
        |  t2.fld_create_date data_fld_create_date,
        |  t2.fld_operate_date data_fld_operate_date,
        |  t2.fld_create_user data_fld_create_user,
        |  t2.fld_busi_type data_fld_busi_type,
        |  t2.fld_total data_fld_total,
        |  t2.fld_amount data_fld_amount,
        |  t2.fld_late_fee data_fld_late_fee,
        |  t2.fld_tax_amount data_fld_tax_amount,
        |  t2.fld_tax data_fld_tax,
        |  t2.fld_cancel data_fld_cancel,
        |  t2.fld_late_amount data_fld_late_amount,
        |  t2.fld_incoming_fee_guid da_fld_incoming_fee_guid
        | from test.xiyang6 t
        | left join da t2
        | on t.fld_guid=t2.fld_owner_fee_guid
        | and t2.fld_cancel!=1
        |""".stripMargin

    spark.sql(s7).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang7")
    spark.stop()
  }
}
