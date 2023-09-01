package raulsantos

import funciones._
import org.apache.spark.sql.functions.{dayofmonth,col}
import scala.concurrent.duration._

object ejercicio12 {

        def main(Args: Array[String]) = {

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark
                        val sales = dfSales(spark)

                        val sales10thDay = sales
                          .filter(dayofmonth(col("date")) === 10)
                        //.foreach(println)

                        val sales10thDayShow = sales10thDay
                          .show(sales10thDay.count.toInt)
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}