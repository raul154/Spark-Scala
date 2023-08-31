package raulsantos

import funciones._
import org.apache.spark.sql.functions._


object ejercicio8{

        def main(args:Array[String]):Unit={

                val spark=crearSpark

                val sales=dfSales(spark)
                val sellers=dfSellers(spark)

                val aportacionMediaCuota=sales.join(sellers,Seq("seller_id"))
                .groupBy("order_id")
                // Con first() podemos recuperar las columnas que hemos perdido al agrupar. Nos quedamos sólo con las que nos interesan para los siguientes pasos
                .agg(first("num_pieces_sold").alias("num_pieces_sold"),first("daily_target").alias("daily_target"),first("seller_id").alias("seller_id"))
                .withColumn("cuotaPorPedido",col("num_pieces_sold")/col("daily_target")*lit(100)) //Aportación de cada pedido a la cuota en %
                .groupBy("seller_id")
                .agg(round(avg("cuotaPorPedido"),4).alias("aporacionMedia")) //Aportación media de un pedido a la cuota de un vendedor
                .orderBy("seller_id")
                .show()
                println("Aportación media de un pedido a la cuota de un vendedor")
        }
}
