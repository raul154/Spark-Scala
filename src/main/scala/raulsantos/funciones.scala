package raulsantos

import org.apache.spark.sql.{DataFrame, SparkSession}

object funciones{

        def crearSpark:SparkSession={
        val spark=SparkSession
        .builder
        .appName("ejercicio1")
        .master("local[*]")
        .getOrCreate()

        spark
        }

        def dfProducts(spark:SparkSession):DataFrame={

        val productos=spark.read
        .option("header","true")
        .option("delimiter",",")
        .csv("data/products.csv")
        .selectExpr("CAST (product_id as INT)",
        "product_name as product_name",
        "CAST(price AS DECIMAL(13,2))")

        productos
        }

        def dfSales(spark:SparkSession):DataFrame={
        val sales=spark.read
        .option("header","true")
        .csv("data/sales.csv")
        .selectExpr("CAST(order_id AS INT) as order_id",
        "CAST(product_id AS INT) as product_id",
        "CAST(seller_id AS INT) as seller_id",
        "CAST(date AS DATE) as date",
        "CAST(num_pieces_sold AS INT) as num_pieces_sold",
        "bill_raw_text",
        "CAST(product_id_num AS INT) as product_id_num")
        sales
        }

        def dfSellers(spark:SparkSession):DataFrame={
        val sellers=spark.read
        .option("header","true")
        .csv("data/sellers.csv")
        .selectExpr("CAST(seller_id AS INT) as seller_id",
        "seller_name",
        "CAST(daily_target AS DECIMAL (13,2)) AS daily_target")

        sellers
        }
}
