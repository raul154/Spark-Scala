package raulsantos

import funciones._
import org.apache.spark.sql.functions._

object ejercicio5{

        def main(args:Array[String])={

                val spark=crearSpark

                //Calculamos el máximo de ventas de un producto

                val maxVentas=dfSales(spark)
                .groupBy("product_id_num")
                .agg(count("*").alias("total_sales"))
                .select(max("total_sales"))
                .first()
                .getLong(0)

                val productosMasVendidos=dfSales(spark)
                .groupBy("product_id_num")
                .agg(count("*").alias("total_sales"))
                .filter(col("total_sales")===maxVentas) // Nos quedamos sólo con los productos que se hayan vendido tantas veces como el máximo
                .show()

              /*.collect() // Transformamos el DF en una matriz

            //Quitando el collect() y con un show() bastaría para obtener el resultado en forma de DF, pero vamos a mostrarlo en una línea con un println()

            //Variable que contendrá una lista con los id de los productos más vendidos
            var idProductosMasVendidos: List[Int] = List()

            //Bucle que recorre la matriz generada a partir del DF
            for (i <- productosMasVendidos) {
              val product_id_num = i.getAs[Int]("product_id_num") //ID de los productos
              idProductosMasVendidos = product_id_num :: idProductosMasVendidos //Añadimos los ID a la lista
            }
            println(s"Los productos más vendidos son: ${idProductosMasVendidos.mkString(", ")}") //el método .mkString() devuelve un string concatenado con los separadores que le
            // pasamos. En este caso será: id1, id2, id3...*/
        }
}
