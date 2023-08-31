package raulsantos

import funciones._

object ejercicio9{

        def main(args:Array[String])={
                val spark=crearSpark

                val products=dfProducts(spark)
                val sales=dfSales(spark)
                val sellers=dfSellers(spark)

                //Hacemos un left join entre products y sales porque, en caso de haber productos que nunca se han vendido, con un inner estaríamos perdiendo información

                val unionDf=products.join(sales,Seq("product_id"),"left_outer").join(sellers,Seq("seller_id"),"inner")
                .orderBy("product_id")
                .show(10)
                println("Unión de los tres datasets")
        }
}
