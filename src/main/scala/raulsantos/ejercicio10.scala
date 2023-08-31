package raulsantos

import funciones._
import org.apache.spark.sql.functions.{sum, col}

object ejercicio10{

        def main(Args:Array[String])={

                val spark=crearSpark
                val sales=dfSales(spark)
                val products=dfProducts(spark)

                // Dinero generado por las ventas

                val gastoTotalEnEuros=sales.join(products,Seq("product_id"))
                .withColumn("gastoPorProducto",col("num_pieces_sold")*col("price")) //Ingresos derivados de la venta de cada producto
                .agg(sum("gastoPorProducto").alias("gastoTotalEnEuros")) // Calculamos la suma total
                .show()
        }
}
