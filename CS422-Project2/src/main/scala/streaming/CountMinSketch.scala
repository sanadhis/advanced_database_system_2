package streaming;
import scala.util.hashing.MurmurHash3

class CountMinSketch(width: Int, rows: Int, arr: Array[Array[Int]]) extends Serializable {
    val wCounters = width
    val dRows = rows
    val cmsArray = arr
    var ips = Set[String]()

    def this(width: Int, rows: Int) = this(width, rows, Array.ofDim[Int](width, rows))

    def hash(ip: String, a: Int) : Int = {
        ( MurmurHash3.stringHash(ip, a) & Int.MaxValue) % wCounters
    }

    def zero(): Array[Array[Int]] = {
        Array.ofDim[Int](dRows, wCounters)
    }

    def map(ip: String) : CountMinSketch = {
        ips += ip
        val newCountMinSketch = new CountMinSketch(wCounters, dRows)
        for { i <- 0 until dRows } newCountMinSketch.cmsArray(i)(hash(ip,i)) = 1
        newCountMinSketch
    }

    def ++(that: CountMinSketch) = {
        (1 to dRows).foreach(i =>
            (1 to wCounters).foreach( j => 
                this.cmsArray(i)(j) += that.cmsArray(i)(j)
            )
        )
        new CountMinSketch(wCounters, dRows, this.cmsArray)
    }

    def estimate(ip: String): Int = {
        val frequencies = getFrequencies(ip)
        frequencies.reduceLeft(_ min _)
    }

    def getIps(): List[String] = {
        ips.toList
    }

    def getFrequencies(ip: String): List[Int] = {
        (1 to dRows).toList.map( d =>
            this.cmsArray(d)(hash(ip,d))
        )
    }

    override def toString = "test with " + wCounters + " width"
}