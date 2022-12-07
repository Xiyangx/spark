package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test5 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val s1 =
      """
        |select t.fld_guid as cbm_fld_guid,t.fld_owner_guid,t.fld_area_guid,t.fld_stop_date as bm_stop_date,t.fld_end_date as bm_end_date,cbfo.fld_guid as cbfo_fld_guid,cbfo.fld_object_guid  from (
        |	select z.* from (
        |		select fld_guid,fld_owner_guid,fld_area_guid,fld_stop_date,fld_end_date, row_number()over(partition by fld_owner_guid,fld_area_guid order by fld_stop_date desc,fld_end_date desc) ranking
        |		from test.es_commerce_bond_main
        |	) z
        |	where z.ranking = 1
        |) t
        |inner join test.es_commerce_bond_fee_object cbfo
        |on cbfo.fld_bond_guid=t.fld_guid
        |""".stripMargin
    spark.sql(s1).createTempView("cbm")

    val s4 =
      """
        |select
        |t1.*,
        |cbm.bm_stop_date,
        |cbm.bm_end_date,
        |cbm.cbm_fld_guid,
        |cbm.cbfo_fld_guid
        | from test.xiyang5 t1
        | left join cbm cbm
        | on cbm.fld_area_guid=t1.fld_area_guid
        | and cbm.fld_owner_guid=t1.fld_owner_guid
        | and cbm.fld_object_guid=t1.fld_object_guid
        |""".stripMargin


    spark.sql(s4).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang6")
    spark.stop()

  }
}
