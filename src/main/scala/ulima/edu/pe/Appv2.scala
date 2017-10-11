package ulima.edu.pe

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Appv2 {
  // Lista de mínimos y máximos aproximados
  var minmaxvals : List[(Double,Double)] = List()

  def main(args : Array[String]) {
    // Configurar e inicializar spark
    val conf = new SparkConf().setAppName("RRHH").setMaster("local")
    val sc = new SparkContext(conf)

    // Validar y leer parámetros
    if (args.length == 0) {
      println("Necesitas ingresar por lo menos el valor de 'k'")
      return
    }
    val k : Int = args(0).toInt
    var index = -1
    if (args.length > 1) {
      index = args(1).toInt
    }

    // RDD inicial a partir de un archivo
    // Previamente se cambiaron los valores alfanuméricos por números representativos.
    // Para las áreas de trabajo:
    // sales -> 0
    // accounting -> 1
    // hr -> 2
    // technical -> 3
    // support -> 4
    // management -> 5
    // IT -> 6
    // product_mng -> 7
    // marketing -> 8
    // RandD -> 9
    // Para los niveles de salario:
    // low -> 0
    // medium -> 1
    // high -> 2
    val rdd0 = sc.textFile("data/HR_comma_sep.csv")

    // Transformación: convertir en un arreglo
    val rdd1 = rdd0.map( x => x.split(","))

    // Además, se almacenaron unos valores mínimos y máximos aproximados
    // según la descripción de cada dato del registro
    // con el fin de que los centroides generados puedan ser más exactos
    minmaxvals = minmaxvals:+((0d, 1d))
    minmaxvals = minmaxvals:+((0d, 10d))
    minmaxvals = minmaxvals:+((0d, 10d))
    minmaxvals = minmaxvals:+((100d, 400d))
    minmaxvals = minmaxvals:+((0d, 10d))
    minmaxvals = minmaxvals:+((0d, 1d))
    minmaxvals = minmaxvals:+((0d, 1d))
    minmaxvals = minmaxvals:+((0d, 1d))
    minmaxvals = minmaxvals:+((0d, 9d))
    minmaxvals = minmaxvals:+((0d, 2d))

    // Si se ingresa como segundo parámetro un index,
    // se debe utilizar solo los valores de ese index para ser evaluado,
    if (index > -1) {
      // por lo tanto los centroides son un solo número
      val centroids = randomNumbers(k, minmaxvals(index)._1, minmaxvals(index)._2)
      // Imprimir en consola los números centroides generados
      println(s"LOS CENTROIDES GENERADOS FUERON:")
      centroids.foreach(c => println(c))

      // Transformación: centroide más cercano como key y
      // como valor una tupla (valor, 1)
      val rdd2 = rdd1.map( x => (getClosestNumber(x(index).toDouble, centroids), (x(index).toDouble, 1)))
      // Transformación: para cada centroide (key), calcular la suma y cantidad de valores
      val rdd3 = rdd2.reduceByKey((x, y) => ( x._1 + y._1 , x._2 + y._2 ))
      // Transformación: calcular el valor promedio para cada cluster
      val rdd4 = rdd3.map(x => (x._1 , 1.0 * x._2._1 / x._2._2))

      // Acción: imprimir cantidades
      rdd3.collect() foreach (x =>
        println(f"EN EL CLUSTER CON CENTROIDE ${x._1}, SE CLASIFICARON ${x._2._2} REGISTROS"))
      // Acción: imprimir promedios
      rdd4.collect() foreach (x =>
        println(f"PARA EL CLUSTER CON CENTROIDE ${x._1}, EL PROMEDIO FUE ${x._2}"))
    } else {
      // Si no se ha ingresado un segundo parámetro
      // se debe realizar la clusterización en base a todas las variables
      // los centroides deben ser de 10 dimensiones
      val centroids = randomPoints(k)
      // Imprimir en consola los puntos centroides generados
      println(s"LOS CENTROIDES GENERADOS FUERON:")
      centroids.foreach(c => println(c))

      // Transformación: centroide más cercano como key y
      // como valor el registro completo
      val rdd2 = rdd1.map( x => (getClosestPoint(
        new Point(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble,
          x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble,
          x(8).toDouble, x(9).toDouble),
        centroids), 1))
      // Transformación: para cada centroide (key), calcular la cantidad de valores clasificados
      val rdd3 = rdd2.reduceByKey((x, y) => x + y)

      // Acción: imprimir cantidades por cluster
      rdd3.collect() foreach (x =>
        println(f"EN EL CLUSTER CON CENTROIDE ${x._1}, SE CLASIFICARON ${x._2} REGISTROS"))
    }
  }

  // Generar k números random dentro de rangos aproximados como centroides iniciales
  def randomNumbers(k: Int, min: Double, max: Double): Array[Double] = {
    val numbers = new Array[Double](k)
    for (i <- 0 until k) {
      if (i == 0) numbers(i) = randomDouble(min, max)
      else {
        var rand = randomDouble(min, max)
        while (numbers.contains(rand)) {
          rand = randomDouble(min, max)
        }
        numbers(i) = rand
      }
    }
    numbers
  }

  def randomDouble(min: Double, max: Double): Double = {
    val range = max - min
    (new Random().nextDouble() * range) + min
  }

  // Encontrar la posición del centroide más cercano a cada valor
  def getClosestNumber(a: Double, centroids: Array[Double]): Double = {
    var minDist = 999999999999999d
    var closest = centroids(0)
    for (i <- centroids.indices) {
      var distance = 0d
      if (a > centroids(i)) distance = a - centroids(i)
      else distance = centroids(i) - a
      if (distance < minDist) {
        minDist = distance
        closest = centroids(i)
      }
    }
    closest
  }

  // Generar k puntos random de 10 dimensiones
  def randomPoints(k: Int): Array[Point] = {
    val points = new Array[Point](k)
    for (i <- 0 until k) {
      points(i) = new Point(randomDouble(minmaxvals.head._1, minmaxvals.head._2),
        randomDouble(minmaxvals(1)._1, minmaxvals(1)._2),
        randomDouble(minmaxvals(2)._1, minmaxvals(2)._2),
        randomDouble(minmaxvals(3)._1, minmaxvals(3)._2),
        randomDouble(minmaxvals(4)._1, minmaxvals(4)._2),
        randomDouble(minmaxvals(5)._1, minmaxvals(5)._2),
        randomDouble(minmaxvals(6)._1, minmaxvals(6)._2),
        randomDouble(minmaxvals(7)._1, minmaxvals(7)._2),
        randomDouble(minmaxvals(8)._1, minmaxvals(8)._2),
        randomDouble(minmaxvals(9)._1, minmaxvals(9)._2))
    }
    points
  }

  // Encontrar la posición del centroide con el punto más cercano a cada valor
  def getClosestPoint(pt: Point, centroids: Array[Point]): Point = {
    var minDist = 999999999999999d
    var closest = centroids(0)
    for (i <- centroids.indices) {
      val distance = pt.squareDistance(centroids(i))
      if (distance < minDist) {
        minDist = distance
        closest = centroids(i)
      }
    }
    closest
  }

  // Clase que define un punto en un espacio de 10 dimensiones
  class Point(val x1: Double, val x2: Double, val x3: Double, val x4: Double, val x5: Double,
    val x6: Double, val x7: Double, val x8: Double, val x9: Double, val x10: Double)
    extends Serializable {
    private def square(v: Double): Double = v * v
    // Método para calcular la distancia cuadrática entre puntos
    def squareDistance(that: Point): Double = {
      square(that.x1 - x1) + square(that.x2 - x2) + square(that.x3 - x3) +
        square(that.x4 - x4) + square(that.x5 - x5) + square(that.x6 - x6) +
        square(that.x7 - x7) + square(that.x8 - x8) + square(that.x9 - x9) +
        square(that.x10 - x10)
    }
    // Método para redondear valores
    private def round(v: Double): Double = (v * 100).toInt / 100.0
    // Mostrar los valores redondeados junto al título del dato
    override def toString = f"(satisfaction_level: ${round(x1)}, last_evaluation: ${round(x2)}, number_project: ${round(x3)}, " +
      f"average_monthly_hours: ${round(x4)}, time_spend_company: ${round(x5)}, work_accident: ${round(x6)}, left: ${round(x7)}, " +
      f"promotion_last_5years: ${round(x8)}, sales: ${round(x9)}, salary: ${round(x10)})"
  }
}
