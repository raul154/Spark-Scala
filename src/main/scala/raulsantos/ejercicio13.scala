package raulsantos

import funciones._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

object ejercicio13 {

        def main(Args: Array[String]) = {

                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = crearSpark

                        val products = dfProducts(spark)
                        val sales = dfSales(spark)
                        val sellers = dfSellers(spark)

                        val todoDf = products.join(sales, Seq("product_id")).join(sellers, Seq("seller_id")) //Join de los tres DF

                        val procesoDfFinal = todoDf
                          .groupBy("order_id")
                          //Ahora recuperamos todas las columnas que nos serán necesarias en los pasos siguientes
                          .agg(first("seller_name").alias("seller_name"), first("product_id").alias("product_id"), first("product_name").alias("product_name"), first("date").alias("date"), first("seller_id").alias("seller_id"), first("num_pieces_sold").alias("num_pieces_sold"), first("daily_target").alias("daily_target"), first("price").alias("price"))
                          .withColumn("seller_idAndName", concat(col("seller_id"), lit("_"), col("seller_name"))) //Concatenamos ID y nombre de los vendedores para el campo VENDEDOR
                          .withColumn("OBJETIVO_VENTA", col("num_pieces_sold") / col("daily_target") * lit(100)) //*100 por ser números muy pequeños
                          .withColumn("INGRESO", col("num_pieces_sold") * col("price"))

                        val mediaIngreso = procesoDfFinal //Calculamos el ingreso medio para obtener el último campo
                          .select(mean("INGRESO").alias("ingresoMedio"))
                          .first()
                          .getDecimal(0)

                        val DfFinal = procesoDfFinal
                          .withColumn("VENTA_DESTACADA", when(col("INGRESO") > mediaIngreso, "Y").otherwise("N"))
                          .selectExpr("CAST (product_id as INT) as ID_PRODUCTO", //Seleccionamos las columnas deseadas con el nombre y los tipos indicados
                                  "product_name as NOMBRE_PRODUCTO",
                                  "order_id as ID_PEDIDO",
                                  "date as FECHA",
                                  "seller_idAndName as VENDEDOR",
                                  "daily_target as OBJETIVO_DIARIO",
                                  "CAST (OBJETIVO_VENTA as DECIMAL (7,2))",
                                  "CAST (INGRESO as DECIMAL (7,2))",
                                  "VENTA_DESTACADA")
                          .coalesce(1)

                        //Guardamos el DF en formato csv y tsv

                        val DfFinalCsv = DfFinal
                          .write
                          .mode("overwrite") //Para sobreescribir el DF si ya existiera otro con el mismo nombre
                          .option("header", "true")
                          .format("csv").save("C:\\Users\\rauls\\OneDrive\\Escritorio\\ejercicio13_csv") //Guardamos el resultado en el escritorio

                        val DfFinalTsv = DfFinal
                          .write
                          .mode("overwrite")
                          .option("header", "true")
                          .option("delimiter", "\t") //Para generar un tsv basta con establecer una tabulación como separador
                          .format("csv").save("C:\\Users\\rauls\\OneDrive\\Escritorio\\ejercicio13_tsv")
                }
                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}
