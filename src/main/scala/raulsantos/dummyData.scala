package raulsantos

import funciones._
import java.time.LocalDate
import com.github.javafaker.Faker
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._ //{DateType, IntegerType, StringType, StructField, StructType}
import java.sql.Date
import org.apache.spark.sql.{DataFrame, Row}
import scala.util.Random
import scala.collection.mutable
import scala.concurrent.duration._


object dummyData {

  def main(args: Array[String]): Unit = {

    val t_ini = System.nanoTime()
    val realizar_calculos = {

      // Establecemos las restricciones de los datos sintéticos
      val desiredSizeInGB = 1 // Tamaño en GB del dataset
      val maxProductId = 99999
      val maxSellerId = 9
      var startDate = LocalDate.of(2020, 7, 11) // El dataset original termina el 10/7/2020, de modo que añadimos días a partir del siguiente
      val maxNumPiecesSold = 100

      // Para utilizar librerías escritas en Java en Scala es necesario crear primero una instancia de las mismas y llamar a
      // sus métodos a partir de esta. Es el caso de Faker, así que empezamos con ello.
      val faker = new Faker()

      // Creamos la sesión de Spark y cargamos el dataset de ventas desde el objeto funciones
      val spark = crearSpark
      val sales = dfSales(spark)

      // Definimos el tipo de datos que tendrá cada columna del data frame creado a partir de datos sintéticos
      val schema = new StructType()
        .add("order_id", IntegerType, nullable = false)
        .add("product_id", IntegerType, nullable = false)
        .add("seller_id", IntegerType, nullable = false)
        .add("date", DateType, nullable = false)
        .add("num_pieces_sold", IntegerType, nullable = false)
        .add("bill_raw_text", StringType, nullable = false)
        .add("product_id_num", IntegerType, nullable = false)

      //Creamos una lista vacía que almacenará objetos de tipo Row con los que posteriormente construiremos el data frame
      val dummyDataRows: mutable.ListBuffer[Row] = mutable.ListBuffer.empty[Row]

      // Función para calcular el tamaño del dataframe en GBs
      def getSizeInGB(data: Iterable[Row]): Double = {
        val bytesPerObject = 8 + 4 + 4 + 12 + 4 + 200 + 4 // Tamaño de un elemento de cada campo en el dataframe
        data.size * bytesPerObject / (1024.0 * 1024.0 * 1024.0)
      }

      //Función para generar una fecha aleatoria desde la última definida en los datos originales hasta los días que indiquemos
      def getRandomDate(startDate: LocalDate, numDays: Long): LocalDate = {
        val randomDays = Random.nextInt(numDays.toInt).toLong //No funcionaba con un nextLong directamente, así que lo hacemos primero con un int y lo transformamos a long
        startDate.plusDays(randomDays)
      }

      // Para no repetir identificador de pedidos, seleccionamos el más alto y le sumamos uno para cada nueva fila
      var orderId = sales.select(max("order_id")).first().getInt(0)

      // Generamos datos nuevos hasta alcanzara el tamaño deseado
      while (getSizeInGB(dummyDataRows) < desiredSizeInGB) {
        orderId += 1
        val productId = faker.number().numberBetween(0, maxProductId + 1)
        val sellerId = faker.number().numberBetween(0, maxSellerId + 1)
        val startDate1 = getRandomDate(startDate, 365 * 3)
        val date = Date.valueOf(startDate1) //transformamos el tipo de fecha para no tener problemas al guarar el DF
        val numPiecesSold = faker.number().numberBetween(1, maxNumPiecesSold + 1)
        val billRawText = faker.regexify("[A-Za-z]{" + 500 + "}")

        // Almacenamos los datos sintéticos en un objeto de tipo row y la añadimos a la lista vacía que definimos previamente
        val rowData: Row = Row(orderId, productId, sellerId, date, numPiecesSold, billRawText, productId)
        dummyDataRows += rowData

        // Para hacer un seguimiento del proceso. Cada 1000 nuevas filas nos indica todas las que ha generado hasta el momento,
        // además del tamaño de los datos sintéticos
        if (dummyDataRows.size % 1000 == 0) {
          println(s"Generando registros... ${dummyDataRows.size} registros generados.  Tamaño de los datos: ${getSizeInGB(dummyDataRows)} GBs.")
        }
      }

      // El método "createDataFrame" de spark necesita un RDD como argumento, de modo que convertimos la lista en uno
      val dummyDataRDD = spark.sparkContext.parallelize(dummyDataRows.toSeq)

      // Creamos el DataFrame usando las filas generadas
      val dummyDataDF: DataFrame = spark.createDataFrame(dummyDataRDD, schema)

      // Unimos los datos sintéticos con el dataframe original "sales"
      val finalDf: DataFrame = sales.union(dummyDataDF).coalesce(1)

      // Guardamos el data frame resultante en un fichero .csv en la ruta que le indiquemos
      val finalDfCsv = finalDf
        .write
        .mode("overwrite") //Para sobreescribir el DF si ya existiera otro con el mismo nombre
        .option("header", "true")
        .format("csv").save("dummy_data/sales_1GB")
    }
    val t_fin = System.nanoTime()
    val duration = Duration(t_fin - t_ini, NANOSECONDS)
    println("Tiempo transcurrido: " + duration.toSeconds + " seconds")
  }
}
