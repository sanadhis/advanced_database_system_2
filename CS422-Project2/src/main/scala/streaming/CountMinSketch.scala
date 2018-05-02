package streaming;
import scala.util.hashing.MurmurHash3

class CountMinSketch(width: Int, rows: Int){
    val wCounters = width
    val dRows = rows

    def hash(x: String) : Int = {
        ( MurmurHash3.stringHash(x, a) & Int.MaxValue) % wCounters
    }

    def zero(): Array[Array[Int]] = {
        Array.ofDim[Int](dRows, wCounters)
    }
}