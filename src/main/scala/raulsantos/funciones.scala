package raulsantos

import org.apache.spark.sql.{DataFrame, SparkSession}

object funciones{

        def crearSpark:SparkSession={

                val spark=SparkSession
                  .builder
                  .appName("ejercicio1")
                  .master("yarn")
                  .getOrCreate()

                spark
        }

        def dfProducts(spark:SparkSession):DataFrame={

                val productos=spark.read
                  .option("header","false")
                  .option("delimiter","\u0001")
                  .csv("hdfs://nameservice1/user/hive/warehouse/TFM_raulsantos.db/products/productss.csv")
                .selectExpr("CAST (_c0 as INT) AS product_id",
                "_c1 as product_name",
                "CAST(_c2 AS DECIMAL(13,2)) AS price")

                productos
        }

        def dfSales(spark:SparkSession):DataFrame={

                val sales=spark.read
                  .option("header","false")
                  .option("delimiter","\u0001")
                  .csv("hdfs://nameservice1/user/hive/warehouse/TFM_raulsantos.db/sales/saless.csv")
                .selectExpr("CAST(_c0 AS INT) as order_id",
                "CAST(_c1 AS INT) as product_id",
                "CAST(_c2 AS INT) as seller_id",
                "CAST(_c3 AS DATE) as date",
                "CAST(_c4 AS INT) as num_pieces_sold",
                "_c5 AS bill_raw_text",
                "CAST(_c6 AS INT) as product_id_num")
                sales
        }

        def dfSalesExtended(spark: SparkSession): DataFrame = {

                val sales = spark.read
                  .option("header", "false")
                  .option("delimiter", "\u0001")
                  .csv("hdfs://nameservice1/user/hive/warehouse/TFM_raulsantos.db/sales/sales_extended_2GB.csv")
                  .selectExpr("CAST(_c0 AS INT) as order_id",
                          "CAST(_c1 AS INT) as product_id",
                          "CAST(_c2 AS INT) as seller_id",
                          "CAST(_c3 AS DATE) as date",
                          "CAST(_c4 AS INT) as num_pieces_sold",
                          "_c5 AS bill_raw_text",
                          "CAST(_c6 AS INT) as product_id_num")
                sales
        }

        def dfSellers(spark:SparkSession):DataFrame={

                val sellers=spark.read
                  .option("header","false")
                  .option ("delimiter", "\u0001")
                  .csv("hdfs://nameservice1/user/hive/warehouse/TFM_raulsantos.db/sellers/sellerss.csv")
                .selectExpr("CAST(_c0 AS INT) AS seller_id",
                "_c1 AS seller_name",
                "CAST(_c2 AS DECIMAL (13,2)) AS daily_target")

                sellers
        }
}