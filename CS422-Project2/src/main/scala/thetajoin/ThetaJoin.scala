package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.math.ceil
import scala.math.abs
import scala.math.sqrt
import scala.util.Sorting.quickSort

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

    val overallRowSize = rdd1Attribute.count().toInt
    val overalColumnSize = rdd2Attribute.count().toInt
    val factor = Math.sqrt(overallRowSize * overalColumnSize / reducers)
  
    val size1 = ceil(overallRowSize / factor).toInt
    val size2 = ceil(overalColumnSize / factor).toInt

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

    horizontalBoundaries = horizontalSamples.toList
    verticalBoundaries = verticalSamples.toList

    val horizontalSamplesMod = 0 +: horizontalBoundaries :+ Int.MaxValue
    val verticalSamplesMod = 0 +: verticalBoundaries :+ Int.MaxValue

    val horizontalBucket = (0 to horizontalBoundaries.size).toList.map( i =>
      rdd1Attribute.filter(row => 
        (row >= horizontalSamplesMod(i)) && (row < horizontalSamplesMod(i+1))
      ).collect().toList
    )

    val verticalBucket = (0 to verticalBoundaries.size).toList.map( i =>
      rdd2Attribute.filter(row => 
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

    // step 2
    // implement histogram
    val histogram = {

      val mockHistogram = Array.fill(horizontalCounts.size){Array.fill(verticalCounts.size){0}}
    
      (horizontalBoundaries :+ Int.MaxValue).zipWithIndex.iterator.foreach( x => {
        (verticalBoundaries :+ Int.MaxValue).zipWithIndex.iterator.filter( y => 
          (x._1 >= verticalSamplesMod(y._2) && x._1 < verticalSamplesMod(y._2 + 1)) || (x._1 >= y._1 && horizontalSamplesMod(x._2) < y._1) ).foreach(y => {
            // define bucket with considered area
            mockHistogram(x._2)(y._2) = 1
            if(op == "<" || op == "<="){
              (y._2 until verticalCounts.size).foreach(yPos => {
                mockHistogram(x._2)(yPos) = 1
              })
            }
            else if(op == ">" || op == ">="){
              (0 until y._2).foreach(yPos => {
                mockHistogram(x._2)(yPos) = 1
              })
            }
        })
      })
      
      mockHistogram
    }

    println()
    histogram.foreach(row => println(row.mkString("_")))
    println()

    // val firstElement = horizontalBucket(0)(0)
    // println("1st element " + firstElement)
    // println("Total of matching 1st element " + histogram(0).sum)

    // find best assignment
    val score = {

      val maxScore = Double.MinValue
      val maxInput = bucketsize

      val nRows = histogram.size
      val nColumns = histogram(0).size
      val assignment = Array.fill(nRows){Array.fill(nColumns){0}}

      (1 to nRows).foreach(rowsThreshold => {
        
        var nBucket = 1
        var reducerId = 1
        var totalRows = 0
        var totalColumn = 0
        var lastColumnIndex = Int.MinValue

        val totalCoverageArea = Array.fill(reducers){0}

        (0 until rowsThreshold).foreach(rows => {

          val rowsCount = horizontalCounts(rows)
          totalRows += rowsCount

          (0 until nColumns).filter(columns => histogram(rows)(columns) == 1).foreach(columns => {
              
              val columnCount = verticalCounts(columns)
              if(columns > lastColumnIndex){
                totalColumn += columnCount
              }
              
              if(totalRows + totalColumn >= maxInput){
                // println("cannot handle condition: " + totalRows + "," + totalColumn)
                reducerId += 1
                nBucket += 1
                totalRows = rowsCount
                totalColumn = columnCount
              }
              
              totalCoverageArea(reducerId) = totalRows*totalColumn  
              // println("can handle condition: " + totalRows + "," + totalColumn)
              assignment(rows)(columns) = reducerId

              lastColumnIndex = columns
          })
          
        })
        
        val score = totalCoverageArea.sum.toDouble / nBucket
        println("Score: " + score  + " for rowsThres=" + rowsThreshold + " with nBucket=" + nBucket)
        
      })

    assignment
    }

    println()    
    score.foreach(row => println(row.mkString("_")))
    println()    
    // step 3, now assign value

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

  def factors(num: Int) : List[Int] = {
    (1 to sqrt(num).toInt).toList.filter{ divisor =>
      num % divisor == 0
    }
  }
  
  def factors_naive(num: Int) : List[Int] = {
    (1 to num).toList.filter{ divisor =>
      num % divisor == 0
    }
  }

}

