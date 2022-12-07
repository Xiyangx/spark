import org.apache.spark.sql.{SaveMode, SparkSession}

object excel {

  def main(args: Array[String]): Unit = {
    <!-- 本地测试设置的一些参数。
spark在实际生产中需要设置的参数还有很多，详细参考官网和实际的应用场景 -->
    val sparkSession: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.warehouse.dir", "hdfs://HDPCluster:8020/user/hive/warehouse")
      //      .config("hive.exec.dynamici.partition", true)
      //      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //      .config("hive.metadata.dml.events", "false")
      //      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled","true")
      .getOrCreate()

    val ds = sparkSession.read.options(Map("useHeader" -> "true")).format("com.crealytics.spark.excel")
      .load("hdfs://HDPCluster:8020/user/hive/warehouse/tablespace/managed/hive/sunac.db/test/WA3V.xlsx")

    //hive表路径或者hive分区路径
    //保存模式有多种, SaveMode.Overwrite为覆盖
    val path = "hdfs://HDPCluster:8020/user/hive/warehouse/tablespace/managed/hive/sunac.db/test_excel_file"
    ds.write.format("parquet").options(Map("" -> "")).mode(SaveMode.Overwrite).save(path)

    // 测试数据是否保存成功。
    //注意：本示例中创建的是非Hive分区表。如果是Hive分区表，还需要执行msck repair table table_name进行分区修复
    sparkSession.sql("select * from test_excel_file").show(10)


  }
}
