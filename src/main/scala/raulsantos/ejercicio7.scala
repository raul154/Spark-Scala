package raulsantos

import funciones._
import org.apache.spark.sql.functions.{col, avg}
import scala.concurrent.duration._

object ejercicio7{

  def main(args:Array[String])= {

    val t_ini = System.nanoTime()
    val realizar_calculos = {

      val spark = crearSpark

      val sales = dfSales(spark)
      val products = dfProducts(spark)

      val gastoMedioPorPedido = sales.join(products, Seq("product_id")) //Hacemos un join entre sales y products, pues necesitamos columnas de ambas tablas
        .withColumn("gastoPorPedido", col("num_pieces_sold") * col("price")) //Nueva columna con el gasto por pedido
        .select(avg("gastoPorPedido").alias("gastoMedio"))
        .first()
        .getDecimal(0)
      println (f"El gasto medio por pedido es $gastoMedioPorPedido")
    }
    val t_fin = System.nanoTime()
    val duration = Duration(t_fin - t_ini, NANOSECONDS)
    println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
  }
}