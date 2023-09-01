package raulsantos

import funciones._
import scala.concurrent.duration._

object ejercicio2 {

        def main(args: Array[String]) {


                val t_ini = System.nanoTime()
                val realizar_calculos = {

                        val spark = funciones.crearSpark

                        val products = dfProducts(spark).show(Int.MaxValue) //Pasamos como argumento a show() el número de filas del DF, quitando así el límite de 20 por
                        //defecto
                        println("Data frame de productos")
                        val sales = dfSales(spark).show(Int.MaxValue)
                        println("Data frame de ventas")
                        val sellers = dfSellers(spark).show(Int.MaxValue)
                        println("Data frame de vendedores")
                }

                val t_fin = System.nanoTime()
                val duration = Duration(t_fin - t_ini, NANOSECONDS)
                println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
        }
}

