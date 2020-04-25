import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._

object karger_algorithm {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("karger_algorithm")
      .master("local[*]")
      .getOrCreate

    import spark.implicits._

    val r = scala.util.Random

    // since its probabilistic need to run multiple times to get the min so loop all below here

    // recreate graph from social network paper -- key connection is 2-4
    val edgesDF = Seq((1,2),(1,3),(2,3),(2,4),(4,5),(4,6),(4,7),(5,6),(6,7)).toDF("vert1", "vert2")

    // original edgesDF
    //val edgesDF = Seq((1,2),(1,3),(1,4),(2,3),(3,4),(4,5)).toDF("vert1", "vert2")
    var best_solution = edgesDF.select("vert1", "vert2")

    for (i <- 1 to 3) {
      println("starting new loop")
      // get df of all vertices - does this need to be an RDD to change whilst we loop over it? Still an array?
      var vertDF = edgesDF.select("vert1").withColumnRenamed("vert1", "vertice").
        union(edgesDF.select("vert2").withColumnRenamed("vert2", "vertice")).distinct()

      var eDF = edgesDF.select(col("vert1"), col("vert2"), col("vert1").as("orig_v1"),col("vert2").as("orig_v2"))

      // loop until only 1 link left
      while (vertDF.count() > 2) {
        // edge to remove index in list chosen randomly

        // edge to remove
        val toCut = scala.math.floor(r.nextFloat() * eDF.count()).toInt
        val v1 = eDF.collect()(toCut)(0)
        val v2 = eDF.collect()(toCut)(1)
        println("edge being cut:", v1, v2)

        // remove second vertix (search by value)
        vertDF = vertDF.filter($"vertice" !== v2)

        // remap edges df version -- leave origs to see solution at end :)
        eDF = eDF.withColumn("vert1", when($"vert1" === v2, v1).otherwise($"vert1")).
          withColumn("vert2", when($"vert2" === v2, v1).otherwise($"vert2")).
          filter(!($"vert1" === v1 && $"vert2" === v1))

        // check verts are removed
        println(vertDF.count())
      }

      if (eDF.count() < best_solution.count()) {
        best_solution = eDF.select("orig_v1", "orig_v2")
      }
    }

    // end looping here and just print the best solution out
    println("Solution has been reached, the cuts required to split the map are:")
    // print each cut it thinks it needs to make
    for (c <- best_solution.collect()) {
      println(c.mkString(" "))
    }
  }
}

/*
    // graphing library time
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.util.IntParam
    import org.apache.spark.graphx.util.GraphGenerators

    // make data structs for vertices and edges
    val vRDD = Seq( 1, 2, 3, 4, 5 ).toDF("id").rdd.map(row => {(row.getInt(0), "blank")})
    val eRDD = Seq( (1,2), (1,3), (1,4), (2,3), (3,4), (4,5) ).toDF("source", "target").rdd.
      map(row => {Edge(row.getInt(0), row.getInt(1))})

    // default value shouldn't ever see this since all id's generated via the total list of sr's
    val default = "Should never see this"

    // turn it into graph
    val graph = Graph(vRDD, eRDD, default)
    val num_vert = graph.numVertices
    val num_edge = graph.numEdges

    println(num_vert)
    println(num_edge)


// basic approach just using dataframes etc
//val vertices = Seq( 1, 2, 3, 4, 5 ).toDF("vertice")
//val edges = Seq( (1,2), (1,3), (1,4), (2,3), (3,4), (4,5) ).toDF("edge")

// importing shit - random library/changeable size array
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

    //var best_solution = ArrayBuffer(Array(123456789))

    // ID,ID, loc in original array (for finding cuts at the end)
    //var non_edit_edges = ArrayBuffer(Array(1, 2, 0), Array(1, 3, 4), Array(1, 4, 2), Array(2, 3, 3), Array(3, 4, 4), Array(4, 5, 6))

    // remap above with numbers for edges place in array (what a mess this is becoming)
    for (i <- 0 to non_edit_edges.length-1) {
      non_edit_edges(i)(2) = i
    }

    // vert ID and copying the network for this run through
      //var vertices = ArrayBuffer(1, 2, 3, 4, 5)
      //var edges = non_edit_edges.map(_.clone)

      //var toCut = scala.math.floor(r.nextFloat() * edges.length).toInt

     // merge vertices together and combine edges as new ones
        //var Array(v1, v2, _) = edges(toCut)

        // remove second vertix (search by value)
        //vertices -= v2

    // remap edges with new vertices and remove any which point to self
        var new_edges = ArrayBuffer[Array[Int]]()
        for (e <- edges) {
          if (e(0) == v2) {
            e(0) = v1
          }
          if (e(1) == v2) {
            e(1) = v1
          }
          // don't add if its a link to itself
          if (!(e(0) == v1 && e(1) == v1)) {
            new_edges += e
          }
        }

  //var new_edges = ArrayBuffer[Array[Int]]()

      // check if this solution is better than the best solution - on first run always replaces
      if (best_solution(0)(0) == 123456789 || edges.length < best_solution.length) {
        best_solution = edges
      }


    for (c <- best_solution.collect()) {
      println(non_edit_edges(c(2)).mkString(" "))
    }

 */
