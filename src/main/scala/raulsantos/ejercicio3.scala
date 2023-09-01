// 3. ¿Cuántos productos, ventas y vendedores hay en total?

package raulsantos

import funciones._
import org.apache.spark.sql.functions.{sum, desc}
import scala.concurrent.duration._

object ejercicio3{

  def main(args:Array[String]) {

    val t_ini = System.nanoTime()
    val realizar_calculos = {

      val spark = funciones.crearSpark

      val numeroProductos = dfProducts(spark).count()

      val numeroSales = dfSales(spark) //En un pedido puede haber varias ventas, de modo que no basta con hacer un count

        .select(sum("num_pieces_sold").alias("totalVentas")) //Nos devuelve un objeto de tipo data frame, por lo que no podemos imprimirlo con println

        .orderBy(desc("totalVentas")) // En un proyecto real con varias personas podría salir un orden distinto a cada uno. En este caso da igual porque hay
        //un único valor, pero es una buena práctica ordenar antes del first()

        .first() //Obtenemos la primera fila del DF, que en este caso es la única. Ahora tenemos un objeto de tipo row (contenedor para una fila de datos con una serie
        //de valores)

        .getLong(0) //Valor de la primera columna de la fila como un objeto de tipo Long

      val numeroSellers = dfSellers(spark).count()

      println(s"La cantidad total de productos es $numeroProductos, la de ventas es $numeroSales y la de vendedores es $numeroSellers")
    }
    val t_fin = System.nanoTime()
    val duration = Duration(t_fin - t_ini, NANOSECONDS)
    println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
  }
}
