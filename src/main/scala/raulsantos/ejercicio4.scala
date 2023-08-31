package raulsantos

import funciones._
import org.apache.spark.sql.functions.count

object ejercicio4{

        def main(args:Array[String])={

                val spark=crearSpark

                val numeroProductosVendidosUnaVez=dfSales(spark)
                .groupBy("product_id_num")
                .agg(count("*").alias("total_sales"))
                .filter("total_sales >= 1")
                .count()

        println(s"El número de productos vendidos al menos una vez es: $numeroProductosVendidosUnaVez")
        }
}
