import scala.io.Source
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.io.BufferedWriter
import scala.collection.JavaConverters._

object Pipe{
  def main(args: Array[String]){
    val command = Seq("cat")
    val pb = new ProcessBuilder(command.asJava)
    val process = pb.start()

    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(process.getOutputStream)))
    out.println("Hello, world!")
    out.close()

    for( line <- Source.fromInputStream(process.getInputStream).getLines()){
      println("--")
      println(line)
    }
  }
}
