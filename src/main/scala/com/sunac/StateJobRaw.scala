package com.sunac

import com.sunac.EndJobFinal.{Job1, Job14, Job15, Job16, Job18, Job19, Job20, Job21_1, Job21_2, Job21_3, Job22}
import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

object StateJobRaw {
  def insertUtilPartition(spark: SparkSession, sql: String, tabName: String): Unit = {
    spark.sql(sql).repartitionByRange(100, col("fld_guid"), rand)
      .foreachPartition((rows: Iterator[Row]) => {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
          connection.setAutoCommit(false)
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_value,create_time) values (?,?,?)".format(tabName))
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_data_src_guid,fld_bill_guid,create_time) values (?,?,?,?)".format(tabName))
          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_value,create_time) values (?,?,?)".format(tabName))

          var size = 0
          while (rows.hasNext) {
            val row: Row = rows.next()
            val fld_guid = row.getAs[String]("fld_guid")
            val fld_value = row.getAs[String]("fld_value")
            //            val fld_bill_guid = row.getAs[String]("fld_bill_guid")
            pstmt.setString(1, fld_guid)
            pstmt.setString(2, fld_value)
            //            pstmt.setString(3, fld_bill_guid)
            pstmt.setObject(3, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
            pstmt.addBatch()
            size += 1
            if ((size % 5000 == 0) && size != 0) {
              pstmt.executeBatch()
              connection.commit()
              pstmt.clearBatch()
            }
          }
          if (size > 0) {
            pstmt.executeBatch()
            connection.commit()
          }
        } catch {
          case e: Exception => {
            println(e)
            if (connection != null) {
              connection.rollback
            }
          }
        }
        finally {
          if (pstmt != null)
            pstmt.close()
          if (connection != null)
            connection.close()
        }
      })
    spark.stop()
  }
  def insertUtilPartition_bill_dim_save(spark: SparkSession, sql: String, tabName: String): Unit = {
    spark.sql(sql).repartitionByRange(100, col("fld_guid"), rand)
      .foreachPartition((rows: Iterator[Row]) => {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
          connection.setAutoCommit(false)
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_value,create_time) values (?,?,?)".format(tabName))
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_data_src_guid,fld_bill_guid,create_time) values (?,?,?,?)".format(tabName))
          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_data_src_guid,fld_bill_guid,create_time) values (?,?,?,?)".format(tabName))

          var size = 0
          while (rows.hasNext) {
            val row: Row = rows.next()
            val fld_guid = row.getAs[String]("fld_guid")
            val fld_data_src_guid = row.getAs[String]("fld_data_src_guid")
            val fld_bill_guid = row.getAs[String]("fld_bill_guid")
            pstmt.setString(1, fld_guid)
            pstmt.setString(2, fld_data_src_guid)
            pstmt.setString(3, fld_bill_guid)
            pstmt.setObject(4, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
            pstmt.addBatch()
            size += 1
            if ((size % 5000 == 0) && size != 0) {
              pstmt.executeBatch()
              connection.commit()
              pstmt.clearBatch()
            }
          }
          if (size > 0) {
            pstmt.executeBatch()
            connection.commit()
          }
        } catch {
          case e: Exception => {
            println(e)
            if (connection != null) {
              connection.rollback
            }
          }
        }
        finally {
          if (pstmt != null)
            pstmt.close()
          if (connection != null)
            connection.close()
        }
      })
    spark.stop()
  }
  def insertUtilPartition_bill(spark: SparkSession, sql: String, tabName: String): Unit = {
    spark.sql(sql).repartitionByRange(100, col("fld_guid"), rand)
      .foreachPartition((rows: Iterator[Row]) => {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
          connection.setAutoCommit(false)
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_value,create_time) values (?,?,?)".format(tabName))
          //          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,fld_data_src_guid,fld_bill_guid,create_time) values (?,?,?,?)".format(tabName))
          pstmt = connection.prepareStatement("insert into `%s` (fld_guid,create_time) values (?,?)".format(tabName))

          var size = 0
          while (rows.hasNext) {
            val row: Row = rows.next()
            val fld_guid = row.getAs[String]("fld_guid")
//            val fld_value = row.getAs[String]("fld_value")
            //            val fld_bill_guid = row.getAs[String]("fld_bill_guid")
            pstmt.setString(1, fld_guid)
//            pstmt.setString(2, fld_value)
            //            pstmt.setString(3, fld_bill_guid)
            pstmt.setObject(2, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
            pstmt.addBatch()
            size += 1
            if ((size % 5000 == 0) && size != 0) {
              pstmt.executeBatch()
              connection.commit()
              pstmt.clearBatch()
            }
          }
          if (size > 0) {
            pstmt.executeBatch()
            connection.commit()
          }
        } catch {
          case e: Exception => {
            println(e)
            if (connection != null) {
              connection.rollback
            }
          }
        }
        finally {
          if (pstmt != null)
            pstmt.close()
          if (connection != null)
            connection.close()
        }
      })
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig_1.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val jobName: String = args(0)
    jobName match {
      case "idx_es_charge_hand_in_record" => Job17(spark)
      case "idx_es_charge_incoming_back" => Job21_1(spark)
      case "idx_es_charge_incoming_convert" => Job21_2(spark)
      case "idx_es_charge_incoming_data_bill" => JobBillJoinSave()
      case "idx_es_charge_incoming_fee" => Job1(spark)
      case "idx_es_charge_incoming_fee1" => Job18(spark)
      case "idx_es_charge_incoming_kou" => Job21_3(spark)
      case "idx_es_charge_join_save_of_b" => JobBill()
      case "idx_es_charge_join_save_of_t" => JobBillType()
      case "idx_es_charge_owner_fee" => Job7()
      case "idx_es_charge_pay_mode" => Job6()
      case "idx_es_charge_project" => Job5()
      case "idx_es_charge_project_period" => Job16(spark)
      case "idx_es_charge_project_period_join" => Job15(spark)
      case "idx_es_charge_settle_accounts_detail" => Job10()
      case "idx_es_charge_settle_accounts_main" => Job14(spark)
      case "idx_es_charge_two_balance" => Job12()
      case "idx_es_charge_voucher_check_service_set" => Job19(spark)
      case "idx_es_charge_voucher_mast_refund_back" => Job22(spark)
      case "idx_es_charge_voucher_mast_refund_convert" => Job23()
      case "idx_es_charge_voucher_mast_refund_kou" => Job24()
      case "idx_es_charge_voucher_project_pay" => Job20(spark)
      case "idx_es_info_area_info" => Job2()
      case "idx_es_info_object" => Job3()
      case "idx_es_info_object_and_owner" => EndJobFinal.Job10(spark)
      case "idx_es_info_object_class" => Job9()
      case "idx_es_info_object_park" => Job8()
      case "idx_es_info_owner" => Job4()
    }
        JobBill()
        JobBillType()
    //    Job2()
    //    Job3()
    //    Job4()
    //    Job5()
    //    Job6()
    //    Job7()
    //    Job8()
    //    Job9()
    //    Job10()
    //    Job11()
    //    Job12()
    //    Job22()
    //    Job23()
    //    Job24()

    //    JobBillDataDimSave()
    JobBillJoinSave()
  }

  def Job17(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_hand_in_record")
    // LEFT JOIN es_charge_two_balance two on two.fld_guid=d.fld_incoming_fee_guid
    // LEFT JOIN es_charge_hand_in_record hir on hir.fld_guid=two.fld_hand_in_guid
    val mainStr =
    """
      |select
      | fld_guid,
      | fld_area_guid,
      | fld_incoming_fee_guid
      | from test.all_tab
      |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql("SELECT fld_guid,fld_hand_in_guid FROM test.two").createTempView("t2")
    val s =
      """
        |select t1.fld_guid,t2.fld_hand_in_guid
        |from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_guid
        |""".stripMargin
    spark.sql(s).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_hand_in_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_hand_in_record")
    //    insertUtil(spark.sql(concatUtil("fld_hand_in_guid")), "idx_es_charge_hand_in_record")
  }
  def JobBillJoinSave(): Unit = {
    truncateTanleUtil("idx_es_charge_incoming_data_bill")
    val spark: SparkSession = SqlConfig.initSpark("JobBillJoinSave")
    val mainStr =
      """
        |select
        | fld_guid
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition_bill(spark, mainStr, "idx_es_charge_incoming_data_bill")
  }

  def JobBillDataDimSave(): Unit = {
    // 12,   LEFT JOIN es_charge_two_balance two on two.fld_guid=d.fld_incoming_fee_guid
    truncateTanleUtil("idx_es_charge_incoming_data_bill_dim_save")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        |fld_guid,
        |fld_data_src_guid,
        |fld_bill_guid
        | from test.table_bill
        |""".stripMargin
    insertUtilPartition_bill_dim_save(spark, mainStr, "idx_es_charge_incoming_data_bill_dim_save")

  }

  def Job7(): Unit = {
    truncateTanleUtil("idx_es_charge_owner_fee")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,md5(concat_ws('',fld_owner_fee_guid,fld_area_guid)) as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_owner_fee")

  }

  def Job8(): Unit = {
    truncateTanleUtil("idx_es_info_object_park")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,md5(concat_ws('',fld_object_guid,fld_area_guid))  as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_info_object_park")
  }

  def Job9(): Unit = {
    truncateTanleUtil("idx_es_info_object_class")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr2 =
      """
        |select
        | t1.fld_guid,
        | t2.fld_class_guid fld_class_guid
        | from t1 join t2 on t1.fld_object_guid=t2.fld_guid
        |""".stripMargin
    spark.sql("select fld_area_guid,fld_guid,fld_object_guid from test.all_tab").createTempView("t1")
    spark.sql("select fld_guid,fld_class_guid,fld_area_guid from test.object").createTempView("t2")
    spark.sql(mainStr2).createTempView("all_tab")
    insertUtilPartition(spark, "select fld_guid,fld_class_guid as fld_value from all_tab", "idx_es_info_object_class")
  }

  def Job10(): Unit = {
    truncateTanleUtil("idx_es_charge_settle_accounts_detail")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,
        | md5(concat_ws('',fld_area_guid,fld_object_guid,fld_owner_guid)) as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_settle_accounts_detail")
  }

  def Job11(): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,
        | md5(concat_ws('',fld_owner_fee_guid,fld_area_guid)) as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_settle_accounts_detail")
  }

  def Job12(): Unit = {
    truncateTanleUtil("idx_es_charge_two_balance")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_incoming_fee_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_two_balance")
  }


  def Job2(): Unit = {
    truncateTanleUtil("idx_es_info_area_info")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_info_area_info")

  }

  def Job3(): Unit = {
    truncateTanleUtil("idx_es_info_object")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_object_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_info_object")

  }

  def Job4(): Unit = {
    truncateTanleUtil("idx_es_info_owner")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_owner_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_info_owner")
  }


  def Job5(): Unit = {
    truncateTanleUtil("idx_es_charge_project")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_project_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_project")
  }

  def Job6(): Unit = {
    truncateTanleUtil("idx_es_charge_pay_mode")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_pay_mode_guid as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, mainStr, "idx_es_charge_pay_mode")
  }



//  def Job22(): Unit = {
//    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
//    val mainStr =
//      """
//        |select
//        | fld_guid,fld_incoming_fee_guid
//        | from test.all_tab
//        |""".stripMargin
//    val ES_CHARGE_INCOMING_BACK: String =
//      """
//        |SELECT
//        |fld_guid,
//        |fld_incoming_back_guid,
//        |substr( fld_submit_time, 1,19 ) fld_submit_time
//        | FROM test.back1
//        |""".stripMargin
//    spark.sql(mainStr).createTempView("t1")
//    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
//    val s8 =
//      """
//        |select
//        |  t1.fld_guid as fld_guid,
//        |  t2.fld_guid as ib_back_fld_guid,
//        |  t2.fld_submit_time as ib_back_submit_time
//        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_back_guid
//        |""".stripMargin
//    spark.sql(s8).createTempView("t8_tmp")
//
//    val s8_ok =
//      """
//        |select m.fld_guid,m.ib_back_fld_guid
//        |   from
//        |(select *,row_number() over ( partition by fld_guid order by ib_back_submit_time desc)back1
//        |  from t8_tmp
//        |)m where m.back1=1
//        |""".stripMargin
//    insertUtilPartition(spark, s8_ok, "idx_es_charge_voucher_mast_refund_back")
//  }

  def Job23(): Unit = {
    truncateTanleUtil("idx_es_charge_voucher_mast_refund_convert")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_INCOMING_BACK: String =
      """
        |select
        |fld_guid,
        |fld_incoming_convert_guid,
        |substr( fld_submit_time, 1,19 ) fld_submit_time
        | FROM test.back1
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
    val s8 =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.fld_guid as ib_convert_fld_guid,
        |  t2.fld_submit_time as ib_back_submit_time
        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_convert_guid
        |""".stripMargin
    spark.sql(s8).createTempView("t8_tmp")
    val s8_ok =
      """
        |select m.fld_guid as fld_guid,m.ib_convert_fld_guid as fld_value
        |   from
        |(select *,row_number() over ( partition by fld_guid order by ib_back_submit_time desc)back1
        |  from t8_tmp
        |)m where m.back1=1
        |""".stripMargin
    insertUtilPartition(spark, s8_ok, "idx_es_charge_voucher_mast_refund_convert")
  }


  def Job24(): Unit = {
    truncateTanleUtil("idx_es_charge_voucher_mast_refund_kou")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid,fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_INCOMING_BACK: String =
      """
        |select
        |fld_guid,
        |fld_incoming_kou_guid,
        |substr( fld_submit_time, 1,19 ) fld_submit_time
        | FROM test.back1
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
    val s8 =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.fld_guid as ib_kou_fld_guid,
        |  t2.fld_submit_time as ib_back_submit_time
        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_kou_guid
        |""".stripMargin
    spark.sql(s8).createTempView("t8_tmp")
    val s8_ok =
      """
        |select m.fld_guid as fld_guid,m.ib_kou_fld_guid as fld_value
        |   from
        |(select *,row_number() over ( partition by fld_guid order by ib_back_submit_time desc)back1
        |  from t8_tmp
        |)m where m.back1=1
        |""".stripMargin
    insertUtilPartition(spark, s8_ok, "idx_es_charge_voucher_mast_refund_kou")
  }

  /** *
   * CREATE TABLE `idx_es_charge_join_save_of_b` (
   * `db_fld_guid` varchar(200) COLLATE utf8mb4_general_ci NOT NULL COMMENT '主键',
   * `fld_guid` varchar(200) COLLATE utf8mb4_general_ci NOT NULL,
   * `fld_bill_guid` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL,
   * `create_time` datetime DEFAULT NULL,
   * `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间',
   * PRIMARY KEY (`db_fld_guid`),
   * KEY `idx_fld_guid` (`fld_guid`),KEY `idx_fld_bill_guid` (`fld_bill_guid`)
   * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
   */
  def JobBill(): Unit = {
    truncateTanleUtil("idx_es_charge_join_save_of_b")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val mainStr =
      """
        |select
        | fld_guid
        | from test.all_tab
        |""".stripMargin
    val bill: String =
      """
        |select
        |fld_guid db_fld_guid,
        |fld_data_src_guid,
        |fld_bill_guid
        |from test.table_bill where fld_data_src_guid IS NOT NULL and fld_bill_guid IS NOT NULL
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(bill).createTempView("t2")
    val sql =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.db_fld_guid,
        |  t2.fld_bill_guid
        |  from t1 join t2 on t1.fld_guid=t2.fld_data_src_guid
        |""".stripMargin
    spark.sql(sql).repartitionByRange(100, col("fld_guid"), rand)
      .foreachPartition((rows: Iterator[Row]) => {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
          connection.setAutoCommit(false)
          pstmt = connection.prepareStatement("insert into `idx_es_charge_join_save_of_b` (db_fld_guid,fld_guid,fld_bill_guid,create_time) values (?,?,?,?)")
          var size = 0
          while (rows.hasNext) {
            val row: Row = rows.next()
            pstmt.setString(1, row.getAs[String]("db_fld_guid"))
            pstmt.setString(2, row.getAs[String]("fld_guid"))
            pstmt.setString(3, row.getAs[String]("fld_bill_guid"))
            pstmt.setObject(4, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
            pstmt.addBatch()
            size += 1
            if ((size % 5000 == 0) && size != 0) {
              pstmt.executeBatch()
              connection.commit()
              pstmt.clearBatch()
            }
          }
          if (size > 0) {
            pstmt.executeBatch()
            connection.commit()
          }
        } catch {
          case e: Exception => {
            println(e)
            if (connection != null) {
              connection.rollback
            }
          }
        }
        finally {
          if (pstmt != null)
            pstmt.close()
          if (connection != null)
            connection.close()
        }
      })
    spark.stop()
  }

  /** *
   * CREATE TABLE `idx_es_charge_join_save_of_t` (
   * `fld_guid` varchar(200) COLLATE utf8mb4_general_ci NOT NULL COMMENT '主表主键',
   * `db_fld_guid` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL,
   * `b_fld_guid` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '关联键',
   * `fld_status` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '关联键',
   * `fld_bill_code` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '关联键',
   * `fld_type_guid` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '关联键',
   * `create_time` datetime DEFAULT NULL,
   * `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间',
   * PRIMARY KEY (`fld_guid`),
   * KEY `idx_fld_guid` (`fld_guid`),KEY `idx_fld_type_guid` (`fld_type_guid`)
   * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
   */
  def JobBillType(): Unit = {
    truncateTanleUtil("idx_es_charge_join_save_of_t")
    println("============================================================================================ok!!!!!!!!!!!!!!!!!!!!!!!")
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    spark.sql(SqlConfig.ES_CHARGE_INCOMING_DATA_BILL).createTempView("table_bill")
    spark.sql(SqlConfig.ES_CHARGE_BILL).createTempView("table_b")
    val s1: String =
      """
        |select m.db_fld_guid,
        |       m.fld_data_src_guid,
        |       m.b_fld_guid,
        |       m.fld_status,
        |       m.fld_bill_code,
        |       m.fld_type_guid
        |from (
        |         select t.*,
        |                row_number() over (partition by t.fld_data_src_guid order by fld_operate_date desc) rowNum
        |         from
        |             (select
        |             bill.db_fld_guid,
        |             bill.fld_data_src_guid,
        |             b.fld_guid b_fld_guid,
        |             b.fld_status,
        |             b.fld_bill_code,
        |             b.fld_type_guid,
        |             b.fld_operate_date
        |             from
        |             (select fld_guid as db_fld_guid, fld_data_src_guid, fld_bill_guid from table_bill where fld_data_src_guid IS NOT NULL and fld_bill_guid IS NOT NULL) bill
        |             join (select fld_guid, fld_status, fld_status, fld_bill_code, fld_type_guid, fld_operate_date from table_b) b
        |             on bill.fld_bill_guid=b.fld_guid
        |             ) t
        |     ) m
        |where m.rowNum = 1
        |""".stripMargin
    spark.sql(s1).createTempView("t1")
    val mainStr =
      """
        |select
        | fld_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("t2")
    val sql =
      """
        |select t2.fld_guid,
        |       t1.db_fld_guid,
        |       t1.b_fld_guid,
        |       cast(t1.fld_status as int) fld_status,
        |       t1.fld_bill_code,
        |       t1.fld_type_guid
        |from t2
        |         join t1 on t2.fld_guid = t1.fld_data_src_guid
        |""".stripMargin
    spark.sql(sql).show(1000, false)
    spark.sql(sql).repartitionByRange(100, col("fld_guid"), rand)
      .foreachPartition((rows: Iterator[Row]) => {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
          connection.setAutoCommit(false)
          pstmt = connection.prepareStatement("insert into idx_es_charge_join_save_of_t (fld_guid,db_fld_guid,b_fld_guid,fld_status,fld_bill_code,fld_type_guid,create_time) values (?,?,?,?,?,?,?)")
          var size = 0
          while (rows.hasNext) {
            val row: Row = rows.next()
            pstmt.setString(1, row.getAs[String]("fld_guid"))
            pstmt.setString(2, row.getAs[String]("db_fld_guid"))
            pstmt.setString(3, row.getAs[String]("b_fld_guid"))
            pstmt.setInt(4, row.getAs[Int]("fld_status"))
            pstmt.setString(5, row.getAs[String]("fld_bill_code"))
            pstmt.setString(6, row.getAs[String]("fld_type_guid"))
            pstmt.setObject(7, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
            pstmt.addBatch()
            size += 1
            if ((size % 5000 == 0) && size != 0) {
              pstmt.executeBatch()
              connection.commit()
              pstmt.clearBatch()
            }
          }
          if (size > 0) {
            pstmt.executeBatch()
            connection.commit()
          }
        } catch {
          case e: Exception => {
            println(e)
            if (connection != null) {
              connection.rollback
            }
          }
        }
        finally {
          if (pstmt != null)
            pstmt.close()
          if (connection != null)
            connection.close()
        }
      })
    spark.stop()
  }

  def truncateTanleUtil(tabName: String): Boolean = {
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true", "root", "UAT_tidb")
    connection.setAutoCommit(false)
    val statement: Statement = connection.createStatement()
    statement.execute("truncate table %s".format(tabName))
    connection.commit()
    statement.close()
    connection.close()
    true
  }
}
