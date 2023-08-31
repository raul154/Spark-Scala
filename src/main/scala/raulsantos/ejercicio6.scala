package raulsantos

import funciones._
import org.apache.spark.sql.functions.asc

object ejercicio6{

        def main(args:Array[String])={

                val spark=crearSpark

                val ventaProductosDistintosDiarios=dfSales(spark)
                .dropDuplicates("date","product_id") //.distinct() quita filas duplicadas. Nosotros sólo queremos quitar duplicados de producto y día
                .groupBy("date")
                .count()
                .orderBy(asc("date"))
                .show()
                println("Número de productos diferentes vendidos cada día")
        }
}
