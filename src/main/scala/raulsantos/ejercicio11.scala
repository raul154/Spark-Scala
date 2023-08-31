package raulsantos

import funciones._
import org.apache.spark.sql.functions._

object ejercicio11{

        def main(Args:Array[String])={

                val spark=crearSpark
                val products=dfProducts(spark)
                val sales=dfSales(spark)

                val eurosPorVendedor=sales.join(products,Seq("product_id"))
                .withColumn("gastoPorProducto",col("num_pieces_sold")*col("price")) // Similar al ejercicio anterior
                .groupBy("seller_id")
                .agg(sum("gastoPorProducto").alias("dineroGenerado")) // Sumamos y obtenemos así la cifra por vendedor
                .orderBy(desc("dineroGenerado"))
                .show()
                println("Dinero generado por vendedor")
        }
}
