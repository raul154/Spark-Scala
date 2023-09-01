package raulsantos

import funciones._
import org.apache.spark.sql.functions.{sum, col}
import scala.concurrent.duration._

object ejercicio10{

        def main(Args:Array[String])= {

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark
                        val sales = dfSales(spark)
                        val products = dfProducts(spark)

                        // Dinero generado por las ventas

                        val gastoTotalEnEuros = sales.join(products, Seq("product_id"))
                          .withColumn("gastoPorProducto", col("num_pieces_sold") * col("price")) //Ingresos derivados de la venta de cada producto
                          .agg(sum("gastoPorProducto").alias("gastoTotalEnEuros")) // Calculamos la suma total
                          .first ()
                          .getDecimal (0)
                        println (f"El gasto total en euros ha sido: $gastoTotalEnEuros")
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " segundos")
        }
}