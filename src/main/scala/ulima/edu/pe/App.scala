package ulima.edu.pe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Random
/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    // Inicializacion
    val conf = new SparkConf().setAppName("ProgramaParcial").setMaster("local")
    val sc = new SparkContext(conf)
    // RDD de un archivo externo
    val archivoDistribuido = sc.textFile("data/HR_comma_sep.csv")
    // Primera transformacion
    val rdd = archivoDistribuido.map( x => x.split(","))


    //rddKeyValue.foreach(println)

    //CALCULAR LOS K CENTROIDES

    def KmeansParcial(k:Int):Array[Double]={
    val r = scala.util.Random
    var i=0
    var j=0
    //DECLARAR ARREGLO DE TAMAÑO K
    var z = new Array[Double](k)
    //LLENAR ARREGLO Z DE RANDOMS QUE NO SE REPITAN
      for(i<-0 to k-1){
        if(i == 0){
          z(i)=r.nextDouble()
        } else {
        for(j <- 0 to i)
            z(i)=r.nextDouble()
          while(z(j)==z(i)){
              z(i)=r.nextDouble()
            }

        }
      }
    return z
    }
  //  var arreglito=KmeansParcial(3)
    //CALCULAR LA POSICION DEL VALOR MAS CERCANO AL VALOR PASADO
    def GetCluster(a:Double,arreglito:Array[Double]):Int={
      var i=0
      var z = new Array[Double](arreglito.length)
      for(i<-0 to arreglito.length-1){
        if(a-arreglito(i)<0){
          z(i)=(-1)*(a-arreglito(i))
        }else{
        z(i)=a-arreglito(i)
        }

      }
      var minimo=z.min
      return z.indexOf(minimo)
    }
    //Segunda transformacion

  def Recursivo(k:Int, n:Int):Int={
    //DECLARACIÓN DE ARREGLOS

    var centroides = KmeansParcial(k)
    var historial = new Array[Double](k)
    var j=0
      for(j <-0 to k-1){
        historial(j) = 1.0
      }

    //COMPARACIÓN DE CENTROIDES

    var valor: Int = 0
    while(valor == 0){

    var z=0
    for(i<-0 to k-1){
      if(centroides(i) == historial(i)){
        z = z+1
      }
    }
    println(z)
    println(k)

    if(z==k){
      valor = 1
    }else{
      valor = 0
    }

   val rddKeyValue = rdd.map(x =>(GetCluster(x(n).toDouble,centroides), (x(n).toDouble,1)))
   val rddSuma = rddKeyValue.reduceByKey((x,y) => ( x._1 + y._1 ,  x._2 + y._2 ))
   val rddProm = rddSuma.map(x => (x._1,1.0 * x._2._1 / x._2._2))

   historial = centroides
   centroides = rddProm.map(_._2).collect()

   }

   val rddFinal = rdd.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),
   x(7),x(8),x(9),"Cluster: " + GetCluster(x(n).toDouble,centroides)))

   println(valor)
   rddFinal.foreach(println)
   println("-----------------------------------------------------------")

   println("ARREGLO HISTORIAL")
   for(u<-historial){
     println(u)
   }
   println("ARREGLO CENTROIDES")
   for(g<-centroides){
     println(g)
   }



   return 0
 }
 println(Recursivo(5,0))


/**
    println("ARREGLO")
    for(s<-arreglito){
      println(s)
    }*/



/**
    println("el mas cercano 0.8")
    println(GetCluster(0.8))
    println("el mas cercano 0.5")
    println(GetCluster(0.5))*/
/**


var minimo=arreglito.min
println(minimo)
println(arreglito.indexOf(minimo))
*/

  }
}
