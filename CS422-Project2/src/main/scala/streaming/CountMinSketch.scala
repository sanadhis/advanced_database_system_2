package streaming;
import scala.util.hashing.MurmurHash3

class CountMinSketch(width: Int, rows: Int, arr: Array[Array[Int]], ipaddresses: Set[String]) extends Serializable {
    val wCounters = width
    val dRows = rows
    val cmsArray = arr
    val ips = ipaddresses

    def this(width: Int, rows: Int) = this(width, rows, Array.ofDim[Int](rows, width), Set[String]())

    def hash(ip: String, a: Int) : Int = {
        ( MurmurHash3.stringHash(ip, a) & Int.MaxValue) % wCounters
    }

    def zero(): Array[Array[Int]] = {
        Array.ofDim[Int](dRows, wCounters)
    }

    def map(ip: String) : CountMinSketch = {
        val zeros = zero()
        for { i <- 0 until dRows } zeros(i)(hash(ip,i)) = 1
        new CountMinSketch(wCounters, dRows, zeros, Set[String](ip))
    }

    def ++(that: CountMinSketch) = {
        (0 until dRows).foreach(i =>
            (0 until wCounters).foreach( j => 
                this.cmsArray(i)(j) += that.cmsArray(i)(j)
            )
        )
        new CountMinSketch(wCounters, dRows, this.cmsArray, this.ips ++ that.ips)
    }

    def estimate(ip: String): Int = {
        val frequencies = getFrequencies(ip)
        frequencies.reduceLeft(_ min _)
    }

    def getIps(): List[String] = {
        ips.toList
    }

    def getFrequencies(ip: String): List[Int] = {
        (0 until dRows).toList.map( d =>
            this.cmsArray(d)(hash(ip,d))
        )
    }
}