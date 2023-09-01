package raulsantos

import funciones._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

object ejercicio5{

        def main(args:Array[String])= {

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark

                        //Calculamos el máximo de ventas de un producto

                        val maxVentas = dfSales(spark)
                          .groupBy("product_id_num")
                          .agg(count("*").alias("total_sales"))
                          .select(max("total_sales"))
                          .first()
                          .getLong(0)

                        val productosMasVendidos = dfSales(spark)
                          .groupBy("product_id_num")
                          .agg(count("*").alias("total_sales"))
                          .filter(col("total_sales") === maxVentas) // Nos quedamos sólo con los productos que se hayan vendido tantas veces como el máximo
                          .show()
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}

