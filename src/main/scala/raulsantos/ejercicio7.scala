package raulsantos

import funciones._
import org.apache.spark.sql.functions.{col, avg}


object ejercicio7{

        def main(args:Array[String])={

                val spark=crearSpark

                val sales=dfSales(spark)
                val products=dfProducts(spark)

                val gastoMedioPorPedido=sales.join(products,Seq("product_id")) //Hacemos un join entre sales y products, pues necesitamos columnas de ambas tablas
                .withColumn("gastoPorPedido",col("num_pieces_sold")*col("price")) //Nueva columna con el gasto por pedido
                .select(avg("gastoPorPedido").alias("gastoMedio"))
                .show()

                println("Gasto medio por pedido")
              /*.first()
              .getDecimal(0) // Valor de la primera columna de la fila como un objeto de tipo Decimal
            println (f"El gasto medio por pedido es $gastoMedioPorPedido")*/
                }
        }
