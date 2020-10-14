import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}


object SparkGraph {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("InputDirectory OutputDirectory")
    }
    val configuration = new SparkConf().setAppName("SparkGraph").setMaster("local")
    val sc = new SparkContext(configuration)

    //Our input dataset is Epinions social network dataset
    val inputPath = if (args.length > 0) args(0) else "soc-Epinions1.txt"
    //Output path
    val outputPath = if (args.length > 1) args(1) else "SparkGraph"

    val input = sc.textFile(inputPath).filter(!_.contains("#")).map(row=>row.split("\t"))

    val vertex = input.map(row=>(row(0).toLong,row(0))).distinct()
    val edge = input.map(row=> Edge(row(0).toLong,row(1).toLong,1))
    val graph = Graph(vertex, edge).persist()


    var od = " Highest OutDegree top 5 nodes and count of number of Outgoing edges for each node in Epinions social network datset " + "\n"
    val outDegree = graph.outDegrees;
    outDegree.sortBy(_._2,false)
      .take(5)
      .foreach(i=>od=od+"\n\t"+" Number of outgoing edges are " +i._2 + " for NodeID " +i._1+ "\n")
    od=od+"\n"



    var id = " Highest InDegree top 5 nodes and count of number of Incoming edges for each node in Epinions social network datset " + "\n"
    val inDegree = graph.inDegrees;
    inDegree.sortBy(_._2,false)
      .take(5)
      .foreach(i=>id=id+"\n\t"+" Number of incoming edges are "+i._2+" for NodeID "+i._1+"\n")
    id=id+"\n"



    var pr = " Highest PageRank values of top 5 nodes in Epinions social network datset " + "\n"
    val pagerank = graph.pageRank(0.00001, resetProb= 0.15).vertices
    pagerank.sortBy(_._2, false)
      .take(5)
      .foreach(i=>pr=pr+"\n\t"+" PageRank is " +i._2 + " for NodeID "+i._1+"\n")
    pr=pr+"\n"


    var cc = " Top 5 Connected Components with the largest number of nodes in Epinions social network datset"+"\n"
    val concomp = graph.connectedComponents().vertices
    concomp.sortBy(_._2,false)
      .take(5)
      .foreach(i=>cc=cc+"\n\t"+" Component "+i._1+" Number of Nodes " +i._2 +"\n")
    cc=cc+"\n"


    var tc = " Largest Triangle Count for top 5 vertices in Epinions social network datset"+"\n"
    val triCount = graph.triangleCount()
      .vertices.sortBy(_._2,false)
      .take(5)
      .foreach(i=>tc=tc+"\n\t" + " Number of Triangles are " +i._2 +" for Vertex "+i._1 +"\n")
    tc=tc+"\n"

    //Writing output to a file
    sc.parallelize(Seq(od + id + pr + cc + tc)).saveAsTextFile(outputPath + " Output ")

    //Printing outputs to run terminal
    print(od + id + pr + cc + tc)
  }
}