package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SqlConfig {
  val prop = new java.util.Properties
  //  prop.setProperty("user", "prod_readonly_for_sjzt")
  //  prop.setProperty("password", "Sunac@1918")
    //prop.setProperty("user", "sjzt_dml")
  prop.setProperty("user", "owner_for_sjzt")
    //prop.setProperty("password", "SJZT_dml")
  prop.setProperty("password", "Sunac@1918")
  prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  //  prop.setProperty("fetchSize", "1000")
  val JDBC_STR  = "jdbc:mysql://10.3.72.83:4000/maindb?characterEncoding=utf-8&rewriteBatchedStatements=true&useCursorFetch=true&autoReconnect=true"
//  val JDBC_STR = "jdbc:mysql://10.3.72.83:4000/maindb?characterEncoding=utf-8"
  //  val JDBC_STR = "jdbc:mysql://172.17.44.84:4000/maindb?characterEncoding=utf-8"
  val JDBC_STR_2 = "jdbc:mysql://10.3.8.231:4000/bj_sjzt_db?characterEncoding=utf-8&rewriteBatchedStatements=true"
  //val JDBC_STR_2 = "jdbc:mysql://10.3.72.88:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true"

  var ES_CHARGE_INCOMING_DATA_BILL: String =
    s"""
       |(SELECT fld_guid,fld_data_src_guid,fld_bill_guid,fld_area_guid
       |FROM es_charge_incoming_data_bill
       |where fld_data_src_guid IS NOT NULL and fld_bill_guid IS NOT NULL)tmp
       |""".stripMargin

  var ES_CHARGE_BILL: String =
    s"""
       |(SELECT fld_guid,fld_status,fld_bill_code,fld_type_guid,date_format(fld_operate_date,'%Y-%m-%d %H:%i:%S') fld_operate_date,fld_area_guid
       |FROM es_charge_bill
       |where fld_type_guid is not null)tmp
       |""".stripMargin

  var ES_CHARGE_BILL_TYPE: String =
    """
      |(SELECT fld_guid,fld_category,fld_name FROM es_charge_bill_type where fld_category in (0,1)) tmp
      |""".stripMargin

  val SLIDE_SQL =
    """
      |(select  %s  FROM  maindb.%s group by %s) tmp
      |""".stripMargin

  val SLIDE_SQL2 =
    """
      |(select date_format( fld_start_date, '%%Y-%%m' )  FROM  maindb.%s group by date_format( fld_start_date, '%%Y-%%m' )) tmp
      |""".stripMargin

  def getBestArray(spark: SparkSession, name: String): Array[String] = {
    val countSQL =
      """
        |(select count(1) AS num FROM maindb.%s) tmp
        |""".stripMargin.format(name)
    val dataCount: Int = spark
      .read
      .format("jdbc")
      .jdbc(JDBC_STR, countSQL, prop).first().getLong(0).asInstanceOf[Int]
    // 获取切片策略：默认是500个分片
    getPartition(dataCount)
      .map {
        case (start, end) => s" fld_guid IN (SELECT fld_guid FROM ${name} LIMIT ${start},${end})"
        //        case (start, end) => s" 1=1 LIMIT ${start},${end})"
      }.filter(arr => arr.length != 2)
  }

  def getPartition(count: Int): Array[(Int, Int)] = {
    val step = count / 2000
    if (step <= 2000) {
      return Array((0, count))
    }
    Range(0, count, step).map(x => {
      (x, step)
    }).toArray
  }

  // 1,
  var ES_CHARGE_INCOMING_DATA_2: String =
    """
      |(select
      |       fld_guid,
      |       fld_start_date,
      |       fld_create_user,
      |       date_format( fld_create_date, '%Y-%m-%d %H:%i:%S' ) fld_create_date,
      |       fld_modify_user,
      |       date_format( fld_modify_date, '%Y-%m-%d %H:%i:%S' ) fld_modify_date,
      |       fld_tenancy,
      |       fld_incoming_fee_guid,
      |       fld_owner_fee_guid,
      |       fld_area_guid,
      |       fld_object_guid,
      |       fld_owner_guid,
      |       fld_project_guid,
      |       fld_pay_mode_guid,
      |       date_format( fld_owner_date, '%Y-%m-%d %H:%i:%S') fld_owner_date
      | from es_charge_incoming_data)tmp
      |""".stripMargin

  // 1,
  var ES_CHARGE_INCOMING_DATA_2_2: String =
    """
      |(select
      |       fld_guid,
      |       fld_start_date,
      |       fld_owner_date,
      |       date_format( fld_allot_date, '%Y-%m-%d %H:%i:%S' ) fld_allot_date,
      |       date_format( fld_finance_date, '%Y-%m-%d %H:%i:%S' ) fld_finance_date,
      |       date_format( fld_point_date, '%Y-%m-%d %H:%i:%S' ) fld_point_date,
      |       fld_desc,
      |       fld_total,
      |       fld_amount,
      |       fld_late_fee,
      |       fld_tax_amount,
      |       fld_tax,
      |       fld_bill_no,
      |       fld_bill_no2
      | from es_charge_incoming_data)tmp
      |""".stripMargin


  var ES_CHARGE_INCOMING_DATA_2_3: String =
    """
      |(select
      |       fld_guid,
      |       fld_area_guid,
      |       fld_operator_guid,
      |       date_format( fld_operate_date, '%Y-%m-%d %H:%i:%S' ) fld_operate_date,
      |       fld_back_fee_guid,
      |       fld_cancel,
      |       fld_start_date,
      |       date_format( fld_start_date, '%Y-%m-%d %H:%i:%S' ) fld_start_date2,
      |       date_format( fld_end_date, '%Y-%m-%d %H:%i:%S' ) fld_end_date,
      |       fld_back_guid
      | from es_charge_incoming_data)tmp
      |""".stripMargin


  var ES_CHARGE_OWNER_FEE_ALL: String =
    """
      |SELECT fld_guid, fld_create_user, fld_create_date, fld_modify_user, fld_modify_date, fld_tenancy, fld_area_guid, fld_adjust_guid, fld_object_guid, fld_owner_guid, fld_project_guid, fld_project_type, fld_total, fld_left_total, fld_amount, fld_rebate, fld_late_total, fld_late_fee, fld_late_date, fld_late_stop, fld_desc, fld_owner_date, fld_allot_date, fld_finance_date, fld_point_date, fld_start_date, fld_end_date, fld_start_read, fld_end_read, fld_number, fld_income, fld_income_source, fld_reason_guid, fld_reason_remark, fld_resource, fld_price, fld_busi_guid
      |FROM test.es_charge_owner_fee
      |""".stripMargin



  // 2,


  var ES_CHARGE_INCOMING_FEE1: String =
    """
      |(SELECT fld_guid,fld_cancel_me,fld_area_guid FROM es_charge_incoming_fee
      | where fld_guid is not null)tmp
      |""".stripMargin

  var ES_INFO_AREA_INFO: String =
    """
      |(SELECT fld_guid,
      |fld_dq,
      |fld_fee_type,
      |date_format(fld_confirm_date,'%Y-%m-%d %H:%i:%S') fld_confirm_date,
      |fld_company,
      |fld_xm,
      |fld_ywdy,
      |fld_yt,
      |fld_name,
      |fld_area_guid FROM es_info_area_info
      | where fld_guid IS NOT NULL)tmp
      |
      |""".stripMargin


  var ES_INFO_OBJECT: String =
    """
      |
      |(SELECT fld_guid,
      |fld_name AS fld_object_name,
      |date_format( if(fld_owner_fee_date<'1900-01-01','1900-01-01',fld_owner_fee_date), '%Y-%m-%d %H:%i:%S' ) fld_owner_fee_date,
      |fld_building,
      |fld_cell,
      |fld_batch,
      |fld_charged_area,
      |fld_status AS fld_obj_status,
      |fld_class_guid,
      |fld_order,
      |fld_status,
      |fld_area_guid,
      |date_format( if(fld_start_fee_date<'1900-01-01','1900-01-01',fld_start_fee_date), '%Y-%m-%d %H:%i:%S' ) fld_start_fee_date
      | FROM es_info_object where fld_guid IS NOT NULL)tmp
      |
      |""".stripMargin
  var ES_CHARGE_INCOMING_DATA: String =
    """
      |(select fld_owner_fee_guid
      |,fld_area_guid
      |,fld_incoming_fee_guid
      |,fld_cancel
      |,fld_guid
      |,fld_create_date
      |,fld_operate_date
      |,fld_create_user
      |,fld_busi_type
      |,fld_total
      |,fld_amount
      |,fld_late_fee
      |,fld_tax_amount
      |,fld_tax
      |,fld_late_amount
      |from es_charge_incoming_data
      |where fld_cancel !=1)tmp
      |""".stripMargin

  var ES_INFO_OWNER: String =

    """
      |(SELECT
      | fld_guid,
      | fld_name AS fld_owner_name,
      | fld_desc AS fld_owner_desc,
      | fld_area_guid
      | FROM es_info_owner
      | where fld_guid IS NOT NULL)tmp
      |""".stripMargin

  var ES_CHARGE_INCOMING_FEE: String =
    """
      |(select  fld_guid,  fld_busi_type,  fld_area_guid,  fld_remark,
      |date_format(fld_cancel_date,'%Y-%m-%d %H:%i:%S') as fld_cancel_date ,
      |fld_cancel_guid,   fld_cancel_me,    fld_checkin,   fld_number
      |from es_charge_incoming_fee ) tmp
      |""".stripMargin

  var ES_CHARGE_PROJECT: String =
    """
      |(SELECT
      |fld_guid,
      |fld_name,
      |fld_object_type
      | FROM es_charge_project where fld_guid IS NOT NULL)tmp
      |""".stripMargin
  var ES_CHARGE_PAY_MODE: String =
    """
      |(SELECT fld_guid,
      |fld_name,
      |fld_resource,
      |fld_pre_pay,
      |fld_attribute FROM es_charge_pay_mode
      | where fld_guid IS NOT NULL)tmp
      |""".stripMargin

  var ES_CHARGE_OWNER_FEE: String =
    """
      |(SELECT fld_guid, fld_create_user, fld_create_date, fld_modify_user, fld_modify_date, fld_tenancy, fld_area_guid, fld_area_name, fld_adjust_guid, fld_object_guid, fld_object_name, fld_owner_guid, fld_owner_name, fld_project_guid, fld_project_name, fld_project_type, fld_total, fld_left_total, fld_amount, fld_rebate, fld_late_total, fld_late_fee, fld_late_date, fld_late_stop, fld_desc, fld_owner_date, fld_allot_date, fld_finance_date, fld_point_date, fld_start_date, fld_end_date, fld_start_read, fld_end_read, fld_number, fld_income, fld_income_source, fld_reason_guid, fld_reason_remark, fld_resource, fld_price, fld_busi_guid
      |from es_charge_owner_fee)tmp
      |""".stripMargin


  var ES_INFO_OBJECT_PARK: String =
    """
      |(SELECT fld_guid,fld_area_guid,fld_cw_category FROM es_info_object_park)tmp
      |""".stripMargin

  var ES_INFO_OBJECT_CLASS: String =
    """
      |(SELECT fld_guid,fld_name FROM es_info_object_class where fld_guid IS NOT NULL)tmp
      |""".stripMargin

  var ES_INFO_OBJECT_AND_OWNER: String =
    """
      |(SELECT fld_guid,fld_room_type,fld_is_owner,fld_area_guid,fld_object_guid,fld_owner_guid,
      |fld_is_current,fld_is_charge,fld_status FROM es_info_object_and_owner
      | where fld_area_guid IS NOT NULL and fld_object_guid IS NOT NULL and fld_owner_guid IS NOT NULL)tmp
      |""".stripMargin

  var ES_CHARGE_SETTLE_ACCOUNTS_DETAIL: String =
    """
      |(SELECT fld_guid,fld_owner_fee_guid,fld_adjust_guid,fld_area_guid,fld_main_guid,fld_status,fld_settle_status,
      |fld_bill_no,date_format(fld_create_date,'%Y-%m-%d %H:%i:%S') fld_create_date FROM es_charge_settle_accounts_detail
      | WHERE fld_status=1 and fld_area_guid is not null)tmp
      |""".stripMargin

  var ES_CHARGE_SETTLE_ACCOUNT_SMAIN: String =
    """
      |(SELECT fld_guid,fld_area_guid,fld_attribute,fld_examine_status
      | FROM es_charge_settle_accounts_main
      | where fld_examine_status=4 and fld_area_guid is not null)tmp
      |""".stripMargin


  var ES_CHARGE_PROJECT_PERIOD_JOIN: String =
    """
      |(SELECT fld_guid,fld_project_guid,fld_period_guid FROM es_charge_project_period_join)tmp
      |""".stripMargin


  var ES_CHARGE_PROJECT_PERIOD_PROJECT: String =
    """
      |(SELECT fld_guid,fld_type,fld_name FROM es_charge_project_period where fld_type=1)tmp
      |""".stripMargin

  var ES_CHARGE_TWO_BALANCE: String =
    """
      |(SELECT fld_guid,date_format( fld_date, '%Y-%m-%d %H:%i:%S' ) fld_date,fld_create_user,fld_hand_in_guid,fld_area_guid FROM es_charge_two_balance)tmp
      |""".stripMargin

  var ES_CHARGE_HAND_IN_RECORD: String =
    """
      |(SELECT fld_guid,fld_hand_in,date_format( fld_start_date, '%Y-%m-%d %H:%i:%S' ) fld_start_date,
      |date_format( fld_end_date, '%Y-%m-%d %H:%i:%S' ) fld_end_date,fld_area_guid
      |FROM es_charge_hand_in_record)tmp
      |""".stripMargin
  var ES_CHARGE_INCOMING_FEE2: String =
    """
      |(SELECT fld_guid,fld_area_guid,
      |date_format(fld_operate_date,'%Y-%m-%d %H:%i:%S') fld_operate_date,
      |fld_cancel,
      |fld_create_user  FROM es_charge_incoming_fee where fld_cancel!= 1 and fld_create_user != "importuser" and fld_create_user != "outuser")tmp
      |""".stripMargin
  var ES_CHARGE_VOUCHER_CHECK_SERVICE_SET: String =
    """
      |(SELECT fld_guid,fld_project_class_guid,fld_type FROM es_charge_voucher_check_service_set where fld_project_class_guid is not null)tmp
      |""".stripMargin
  var ES_CHARGE_VOUCHER_PROJECT_PAY: String =
    """
      |(SELECT fld_guid,fld_pay_mode_guid  FROM es_charge_voucher_project_pay where fld_pay_mode_guid is not null)tmp
      |""".stripMargin

  var ES_CHARGE_INCOMING_BACK: String =
    """
      |(SELECT
      |fld_area_guid,
      |fld_guid,
      |fld_incoming_back_guid,
      |fld_incoming_convert_guid,
      |fld_incoming_kou_guid,
      |date_format( fld_submit_time, '%Y-%m-%d %H:%i:%S' ) fld_submit_time
      | FROM es_charge_incoming_back)tmp
      |""".stripMargin

  var ES_CHARGE_VOUCHER_MAST_REFUND: String =
    """
      |(select fld_guid,
      |fld_incoming_back_guid,
      |date_format( fld_appay_date, '%Y-%m-%d %H:%i:%S' ) fld_appay_date,
      |date_format( fld_date, '%Y-%m-%d %H:%i:%S' ) fld_date
      | FROM es_charge_voucher_mast_refund where fld_incoming_back_guid is not null)tmp
      |""".stripMargin

  def insertHiveUtil(df: DataFrame, name: String) = {
    df.repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test." + name)
  }
  val INSERT_ALL_SQL_2 =
    """
      |SELECT fld_guid,
      |fld_create_user,
      |fld_create_date,
      |fld_modify_user,
      |fld_modify_date,
      |fld_tenancy,
      |fld_area_guid,
      |fld_area_name,
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
      |from test.xiyang11
      |""".stripMargin

  val INSERT_ALL_SQL_1 =
    """
      |SELECT fld_guid, fld_create_user, fld_create_date, fld_modify_user, fld_modify_date, fld_tenancy, fld_area_guid, fld_area_name, fld_adjust_guid, fld_object_guid, fld_object_name, fld_owner_guid, fld_owner_name, fld_project_guid, fld_project_name, fld_project_type, fld_total, fld_left_total, fld_amount, fld_rebate, fld_late_total, fld_late_fee, fld_late_date, fld_late_stop, fld_desc, fld_owner_date, fld_allot_date, fld_finance_date, fld_point_date, fld_start_date, fld_end_date, fld_start_read, fld_end_read, fld_number, fld_income, fld_income_source, fld_reason_guid, fld_reason_name, fld_reason_remark, fld_resource, fld_price, fld_busi_guid, fld_dq, fld_ywdy, fld_xm, fld_company, fld_confirm_date, fld_fee_type, fld_is_owner, fld_owner_fee_date, fld_yt, fld_object_class_name, fld_project_period_name, fld_settle_status, fld_attribute, fld_settle_bill_no, fld_settle_adjust_guid, fld_owner_fee_guid, fld_main_guid, fld_examine_status, fld_batch, fld_building, fld_cell, fld_charged_area, fld_obj_status, fld_ticket_status, fld_co_bill_no, fld_object_type, fld_rate, fld_taxes, fld_start_fee_date, fld_phone_number, fld_owner_desc, bm_stop_date, bm_end_date, data_fld_create_date, data_fld_operate_date, data_fld_create_user, data_fld_busi_type, fee_fld_cancel_me, fld_examine_date, obj_fld_order, data_fld_total, data_fld_amount, data_fld_late_fee, data_fld_tax_amount, data_fld_tax, data_fld_cancel, data_fld_late_amount, a_fld_guid, o_fld_guid, cif_fld_guid, w_fld_guid, p_fld_guid, c_fld_guid, ecrr_fld_guid, pj_fld_guid, pp_fld_guid, ad_fld_guid, am_fld_guid, ct_fld_guid, co_fld_guid, da_fld_guid, oao_fld_guid, cbfo_fld_guid, cbm_fld_guid, ecrr_fld_general_tax, pj_fld_project_guid, pj_fld_period_guid
      |FROM test.xiyang10
      |""".stripMargin

  val INSERT_ALL_SQL =
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
      |if(fld_dq is null,'',fld_dq) fld_dq,
      |if(fld_ywdy is null,'',fld_ywdy) fld_ywdy,
      |if(fld_xm is null,'',fld_xm) fld_xm,
      |if(fld_company is null,'',fld_company) fld_company,
      |if(fld_confirm_date is null,'1970-01-01',fld_confirm_date) fld_confirm_date,
      |if(fld_fee_type is null,'',fld_fee_type) fld_fee_type,
      |if(fld_is_owner is null,99,fld_is_owner) fld_is_owner,
      |if(fld_owner_fee_date is null,'',fld_owner_fee_date) fld_owner_fee_date,
      |if(fld_yt is null,'',fld_yt) fld_yt,
      |if(fld_object_class_name is null,'',fld_object_class_name) fld_object_class_name,
      |if(fld_project_period_name is null,'',fld_project_period_name) fld_project_period_name,
      |if(fld_settle_status is null,99,fld_settle_status) fld_settle_status,
      |if(fld_attribute is null,99,fld_attribute) fld_attribute,
      |if(fld_settle_bill_no is null,'',fld_settle_bill_no) fld_settle_bill_no,
      |if(fld_settle_adjust_guid is null,'',fld_settle_adjust_guid) fld_settle_adjust_guid,
      |if(fld_owner_fee_guid is null,'',fld_owner_fee_guid) fld_owner_fee_guid,
      |if(fld_main_guid is null,'',fld_main_guid) fld_main_guid,
      |if(fld_examine_status is null,99,fld_examine_status) fld_examine_status,
      |if(fld_batch is null,'',fld_batch) fld_batch,
      |if(fld_building is null,'',fld_building) fld_building,
      |if(fld_cell is null,'',fld_cell) fld_cell,
      |if(fld_charged_area is null,0,fld_charged_area) fld_charged_area,
      |if(fld_obj_status is null,99,fld_obj_status) fld_obj_status,
      |if(fld_ticket_status is null,99,fld_ticket_status) fld_ticket_status,
      |if(fld_co_bill_no is null,'',fld_co_bill_no) fld_co_bill_no,
      |if(fld_object_type is null,99,fld_object_type) fld_object_type,
      |if(fld_rate is null,0,fld_rate) fld_rate,
      |if(fld_taxes is null,0,fld_taxes) fld_taxes,
      |if(fld_start_fee_date is null,'',fld_start_fee_date) fld_start_fee_date,
      |if(fld_phone_number is null,'',fld_phone_number) fld_phone_number,
      |if(fld_owner_desc is null,'',fld_owner_desc) fld_owner_desc,
      |if(bm_stop_date is null,'1970-01-01',bm_stop_date) bm_stop_date,
      |if(bm_end_date is null,'1970-01-01',bm_end_date) bm_end_date,
      |if(data_fld_create_date is null,'1970-01-01',data_fld_create_date) data_fld_create_date,
      |if(data_fld_operate_date is null,'1970-01-01',data_fld_operate_date) data_fld_operate_date,
      |if(data_fld_create_user is null,'',data_fld_create_user) data_fld_create_user,
      |if(data_fld_busi_type is null,99,data_fld_busi_type) data_fld_busi_type,
      |if(fee_fld_cancel_me is null,'',fee_fld_cancel_me) fee_fld_cancel_me,
      |if(fld_examine_date is null,'1970-01-01',fld_examine_date) fld_examine_date,
      |if(obj_fld_order is null,99,obj_fld_order) obj_fld_order,
      |if(data_fld_total is null,0,data_fld_total) data_fld_total,
      |if(data_fld_amount is null,0,data_fld_amount) data_fld_amount,
      |if(data_fld_late_fee is null,0,data_fld_late_fee) data_fld_late_fee,
      |if(data_fld_tax_amount is null,0,data_fld_tax_amount) data_fld_tax_amount,
      |if(data_fld_tax is null,0,data_fld_tax) data_fld_tax,
      |if(data_fld_cancel is null,99,data_fld_cancel) data_fld_cancel,
      |if(data_fld_late_amount is null,0,data_fld_late_amount) data_fld_late_amount,
      |if(a_fld_guid is null,'',a_fld_guid) a_fld_guid,
      |if(o_fld_guid is null,'',o_fld_guid) o_fld_guid,
      |if(cif_fld_guid is null,'',cif_fld_guid) cif_fld_guid,
      |if(w_fld_guid is null,'',w_fld_guid) w_fld_guid,
      |if(p_fld_guid is null,'',p_fld_guid) p_fld_guid,
      |if(c_fld_guid is null,'',c_fld_guid) c_fld_guid,
      |if(ecrr_fld_guid is null,'',ecrr_fld_guid) ecrr_fld_guid,
      |if(pj_fld_guid is null,'',pj_fld_guid) pj_fld_guid,
      |if(pp_fld_guid is null,'',pp_fld_guid) pp_fld_guid,
      |if(ad_fld_guid is null,'',ad_fld_guid) ad_fld_guid,
      |if(am_fld_guid is null,'',am_fld_guid) am_fld_guid,
      |if(ct_fld_guid is null,'',ct_fld_guid) ct_fld_guid,
      |if(co_fld_guid is null,'',co_fld_guid) co_fld_guid,
      |if(oao_fld_guid is null,'',oao_fld_guid) oao_fld_guid,
      |'' as cbfo_fld_guid,
      |'' as cbm_fld_guid,
      |if(ecrr_fld_general_tax is null,'',ecrr_fld_general_tax) ecrr_fld_general_tax,
      |'' as pj_fld_project_guid,
      |if(pj_fld_period_guid is null,'',pj_fld_period_guid) pj_fld_period_guid
      |from test.xiyang8
      |""".stripMargin

  def initSpark(name: String): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "hive")
    SparkSession.builder()
      .appName(name)
      .config("spark.sql.warehouse.dir", "hdfs://HDPCluster:8020/user/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.blacklist.enabled", "true")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.yarn.max.executor.failures", 3)
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
//      .config("spark.sql.parquet.writeLegacyFormat","true")
      //.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60")
      //      .set("spark.dynamicAllocation.executorIdleTimeout", "60") //executor闲置时间
      //      .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60") //cache闲置时间
      .config("spark.task.maxFailures", 10)
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    println(ES_CHARGE_VOUCHER_MAST_REFUND)
  }
}
