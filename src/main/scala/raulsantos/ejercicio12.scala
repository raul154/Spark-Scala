package raulsantos

import funciones._
import org.apache.spark.sql.functions.{dayofmonth,col}

object ejercicio12{

        def main(Args:Array[String])={

                val spark=crearSpark
                val sales=dfSales(spark)

                val sales10thDay=sales
                .filter(dayofmonth(col("date"))===10)
                //.foreach(println)

                val sales10thDayShow=sales10thDay
                .show(sales10thDay.count.toInt)

                //Si quisiéramos mostrar el DF filtrado con un show bastaría con comentar la línea 15 y aplicar el siguiente código

            /*val sales10thDayShow = sales10thDay
              .show(sales10thDay.count.toInt)*/
        }
}
