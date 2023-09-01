package raulsantos

import funciones._
import org.apache.spark.sql.functions.asc
import scala.concurrent.duration._

object ejercicio6{

        def main(args:Array[String])={

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark

                        val ventaProductosDistintosDiarios = dfSales(spark)
                          .dropDuplicates("date", "product_id") //.distinct() quita filas duplicadas. Nosotros sólo queremos quitar duplicados de producto y día
                          .groupBy("date")
                          .count()
                          .orderBy(asc("date"))
                          .show()
                        println("Número de productos diferentes vendidos cada día")
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}