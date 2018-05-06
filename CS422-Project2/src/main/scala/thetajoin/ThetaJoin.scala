package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.math.ceil
import scala.util.Sorting.quickSort
import scala.math.abs


class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")    
  
  // random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries = List[Int]()
  var verticalBoundaries = List[Int]()
  
  // number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts = List[Int]()
  var verticalCounts = List[Int]()      
  
  /*
   * this method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */  
  def theta_join(dataset1: Dataset, dataset2: Dataset, attr1:String, attr2:String, op:String): RDD[(Int, Int)] = {
    val schema1 = dataset1.getSchema
    val schema2 = dataset2.getSchema        
    
    val rdd1 = dataset1.getRDD
    val rdd2 = dataset2.getRDD
    
    val index1 = schema1.indexOf(attr1)
    val index2 = schema2.indexOf(attr2)        
    
    val rdd1Attribute = rdd1.map(row => row.getInt(index1))
    val rdd2Attribute = rdd2.map(row => row.getInt(index2))

    var maxInput = 100
    val r = reducers // same as reducers
    val rowSize = rdd1Attribute.count()
    val columnSize = rdd2Attribute.count()
    val factor = Math.sqrt(rowSize * columnSize / reducers)
  
    val size1 = ceil(rowSize / factor).toInt
    val size2 = ceil(columnSize / factor).toInt

    // step 1: sampling
    var horizontalSamples = rdd1Attribute.takeSample(false, size1)
    var verticalSamples = rdd2Attribute.takeSample(false, size2)    

    while(
      generalCheck(horizontalSamples) || checkValue(horizontalSamples)
    ) { horizontalSamples = rdd1Attribute.takeSample(false, size1) }

    while(
      generalCheck(verticalSamples) || checkValue(verticalSamples)
    ) { verticalSamples = rdd2Attribute.takeSample(false, size2) }

    quickSort(horizontalSamples)
    quickSort(verticalSamples)

    val horizontalSamplesMod = 0 +: horizontalSamples :+ Int.MaxValue
    val verticalSamplesMod = 0 +: verticalSamples :+ Int.MaxValue

    horizontalBoundaries = horizontalSamples.toList
    verticalBoundaries = verticalSamples.toList

    val horizontalBucket = (0 to horizontalBoundaries.size).toList.map( i =>
      rdd1Attribute.filter(row => 
        (row >= horizontalSamplesMod(i)) && (row < horizontalSamplesMod(i+1))
      ).collect().toList
    )

    val verticalBucket = (0 to verticalBoundaries.size).toList.map( i =>
      rdd1Attribute.filter(row => 
        (row >= verticalSamplesMod(i)) && (row < verticalSamplesMod(i+1))
      ).collect().toList
    )

    horizontalCounts = horizontalBucket.map( bucket =>
      bucket.size
    )

    verticalCounts = verticalBucket.map( bucket =>
      bucket.size
    )

    println("Horizontal boundaries: " + horizontalBoundaries)
    println("Vertical boundaries: " + verticalBoundaries)
    println("Horizontal count: " + horizontalCounts)
    println("Vertical count: " + verticalCounts)

    // done step 1

    null
  }  
    
  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[(Int, Int)], dat2:Iterator[(Int, Int)], op:String) : Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    var dat2List = dat2.toList
        
    while(dat1.hasNext) {
      val row1 = dat1.next()      
      for(row2 <- dat2List) {
        if(checkCondition(row1._2, row2._2, op)) {
          res = res :+ (row1._2, row2._2)
        }        
      }      
    }    
    res.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }    

  def generalCheck(sample: Array[Int]): Boolean = {
    sample.contains(0) || (sample.size > sample.toSet.size) || sample.min < 10
  }

  def checkValue(sample: Array[Int]): Boolean = {
    val min = sample.combinations(2).map(arr => math.abs(arr(0) - arr(1)) ).toArray.min
    min < 100
  }
}

