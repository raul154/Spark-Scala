package raulsantos

import funciones._

object ejercicio1 {

  def main(args: Array[String]) {

    val spark = funciones.crearSpark

    //Cargamos los datasets, ordenamos por un campo y mostramos los primeros cinco registros

    val fiveProducts = funciones.dfProducts(spark).orderBy("product_id").show(5, truncate = false)
    println ("First five products")
    val fiveSales = funciones.dfSales(spark).orderBy("order_id").show(5, truncate = false)
    println ("First five sales")
    val fiveSellers = funciones.dfSellers(spark).orderBy("seller_id").show(5, truncate = false)
    println ("First five sellers")
  }
}
