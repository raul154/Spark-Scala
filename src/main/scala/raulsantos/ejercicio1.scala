package raulsantos

import funciones._
import scala.concurrent.duration._

object ejercicio1 {

  def main(args: Array[String]) {

    val t_ini = System.nanoTime()
    val realizar_calculos = {

      val spark = funciones.crearSpark

      //Cargamos los datasets, ordenamos por un campo y mostramos los primeros cinco registros

      val fiveProducts = funciones.dfProducts(spark).orderBy("_c0").show(5, truncate = false)
      println("First five products")
      val fiveSales = funciones.dfSales(spark).orderBy("_c0").show(5, truncate = false)
      println("First five sales")
      val fiveSellers = funciones.dfSellers(spark).orderBy("_c0").show(5, truncate = false)
      println("First five sellers")

      val t_fin = System.nanoTime()
      val duration = Duration(t_fin - t_ini, NANOSECONDS)
      println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
    }
  }
}