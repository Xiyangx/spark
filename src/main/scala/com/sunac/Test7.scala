package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test7 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))

    val s7 =
      """
        |select
        |  t.*,
        |  t2.fld_guid as cif_fld_guid,
        |  t2.fld_cancel_me fee_fld_cancel_me
        | from test.xiyang7 t
        | left join test.es_charge_incoming_fee t2
        | on t.da_fld_incoming_fee_guid=t2.fld_guid
        |""".stripMargin

    spark.sql(s7).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang9")

    val s8 =
      """
        |SELECT fld_guid,
        |fld_create_user,
        |fld_create_date,
        |fld_modify_user,
        |fld_modify_date,
        |fld_tenancy,
        |fld_area_guid,
        |fld_name as fld_area_name,
        |fld_adjust_guid,
        |fld_object_guid,
        |fld_object_name,
        |fld_owner_guid,
        |fld_owner_name,
        |fld_project_guid,
        |fld_project_name,
        |fld_project_type,
        |fld_total,
        |fld_left_total,
        |fld_amount,
        |fld_rebate,
        |fld_late_total,
        |fld_late_fee,
        |fld_late_date,
        |fld_late_stop,
        |fld_desc,
        |fld_owner_date,
        |fld_allot_date,
        |fld_finance_date,
        |fld_point_date,
        |fld_start_date,
        |fld_end_date,
        |fld_start_read,
        |fld_end_read,
        |fld_number,
        |fld_income,
        |fld_income_source,
        |fld_reason_guid,
        |fld_reason_name,
        |fld_reason_remark,
        |fld_resource,
        |fld_price,
        |fld_busi_guid,
        |fld_dq,
        |fld_ywdy,
        |fld_xm,
        |fld_company,
        |fld_confirm_date,
        |fld_fee_type,
        |fld_is_owner,
        |fld_owner_fee_date,
        |fld_yt,
        |fld_object_class_name,
        |fld_project_period_name,
        |fld_settle_status,
        |fld_attribute,
        |fld_settle_bill_no,
        |fld_settle_adjust_guid,
        |fld_owner_fee_guid,
        |fld_main_guid,
        |fld_examine_status,
        |fld_batch,
        |fld_building,
        |fld_cell,
        |fld_charged_area,
        |fld_obj_status,
        |fld_ticket_status,
        |fld_co_bill_no,
        |fld_object_type,
        |fld_rate,
        |fld_taxes,
        |fld_start_fee_date,
        |fld_phone_number,
        |fld_owner_desc,
        |bm_stop_date,
        |bm_end_date,
        |data_fld_create_date,
        |data_fld_operate_date,
        |data_fld_create_user,
        |data_fld_busi_type,
        |fee_fld_cancel_me,
        |fld_examine_date,
        |obj_fld_order,
        |data_fld_total,
        |data_fld_amount,
        |data_fld_late_fee,
        |data_fld_tax_amount,
        |data_fld_tax,
        |data_fld_cancel,
        |data_fld_late_amount,
        |a_fld_guid,
        |o_fld_guid,
        |cif_fld_guid,
        |w_fld_guid,
        |p_fld_guid,
        |c_fld_guid,
        |ecrr_fld_guid,
        |pj_fld_guid,
        |pp_fld_guid,
        |ad_fld_guid,
        |am_fld_guid,
        |ct_fld_guid,
        |co_fld_guid,
        |oao_fld_guid,
        |cbfo_fld_guid,
        |cbm_fld_guid,
        |ecrr_fld_general_tax,
        |pj_fld_project_guid,
        |pj_fld_period_guid
        |from test.xiyang9
        |""".stripMargin
    spark.sql(s8).repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang11")
    spark.stop()
  }
}
