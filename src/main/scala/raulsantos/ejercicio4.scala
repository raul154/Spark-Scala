package raulsantos

import funciones._
import org.apache.spark.sql.functions.count
import scala.concurrent.duration._

object ejercicio4{

        def main(args:Array[String])= {

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark

                        val numeroProductosVendidosUnaVez = dfSales(spark)
                          .groupBy("product_id_num")
                          .agg(count("*").alias("total_sales"))
                          //.filter("total_sales >= 1") // No es necesario porque elegir el dataset de ventas ya filtra aquellos productos que no han sido vendidos
                          .count()

                        println(s"El número de productos vendidos al menos una vez es: $numeroProductosVendidosUnaVez")
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}

