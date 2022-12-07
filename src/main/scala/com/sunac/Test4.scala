package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test4 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))

    val s =
      """
        |select fld_guid,fld_operate_guid,fld_owner_fee_guid
        |from(
        |select *, row_number()over(partition by fld_owner_fee_guid order by fld_create_date desc) ranking
        |from test.es_charge_ticket_pay_detail
        |) t where t.ranking = 1;
        |""".stripMargin
    spark.sql(s).createTempView("ct")
    val s1 =
      """
        |select t.*,
        |t2.fld_guid as ct_fld_guid,
        |t2.fld_owner_fee_guid as ct_fld_owner_fee_guid,
        |t2.fld_operate_guid as ct_fld_operate_guid
        | from test.xiyang4 t
        | left join ct t2
        | on t.fld_guid=t2.fld_owner_fee_guid
        |""".stripMargin
    spark.sql(s1).createTempView("ctOk")
    val s2 =
      """
        |select t1.*,
        |t2.fld_guid as co_fld_guid,
        |t2.fld_ticket_status,
        |t2.fld_bill_no as fld_co_bill_no
        | from ctOk t1
        | left join test.es_charge_ticket_pay_operate t2
        | on t1.ct_fld_operate_guid=t2.fld_guid
        |""".stripMargin
//    spark.sql(s2).createTempView("coOk")

    spark.sql(s2).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang5")
    spark.stop()
  }
}
