package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
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
  
    val size1 = ceil(overallRowSize / factor).toInt * 10
    val size2 = ceil(overalColumnSize / factor).toInt * 10

    // step 1: sampling
    var horizontalSamples = Array.fill(size1){0}
    var verticalSamples = Array.fill(size2){0}

    for { i <- 0 until horizontalSamples.size } {
      var sampleValue = rdd1Attribute.takeSample(false, 1)(0)
      
      while (horizontalSamples.contains(sampleValue)) {
        sampleValue = rdd1Attribute.takeSample(false, 1)(0)
      }

      horizontalSamples(i) = sampleValue
    }

    for { i <- 0 until verticalSamples.size } {
      var sampleValue = rdd2Attribute.takeSample(false, 1)(0)
        
      while (verticalSamples.contains(sampleValue)) {
        sampleValue = rdd2Attribute.takeSample(false, 1)(0)
      }
      
      verticalSamples(i) = sampleValue
    }

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

    // find best assignment
    var nBucket = 1
    val bestAssignment = {

      val maxScore = Double.MinValue
      val maxInput = bucketsize

      val nRows = histogram.size
      val nColumns = histogram(0).size
      val assignment = Array.fill(nRows){Array.fill(nColumns){0}}

      (1 to nRows).foreach(rowsThreshold => {
        
        nBucket = 1
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
    bestAssignment.foreach(row => println(row.mkString("_")))
    println()

    // step 3, now assign value
    val leftAssignment = bestAssignment
    val rightAssignment = bestAssignment.transpose

    // left assignment
    val leftRDDAssignment = rdd1.flatMap(row => {
      val position = search(row.getInt(index1), horizontalSamplesMod)
      leftAssignment(position).distinct.filter(assignment => assignment != 0).map( reducerId => (reducerId, ("L", row) ) )
    })

    // right assignment
    val rightRDDAssignment = rdd2.flatMap(row => {
      val position = search(row.getInt(index2), verticalSamplesMod)
      rightAssignment(position).distinct.filter(assignment => assignment != 0).map( reducerId => (reducerId, ("R", row) ) )
    })

    // leftRDDAssignment.foreach(e => println(e))
    // rightAssignment.foreach(e => println(e))

    // step 4, partition value based on bucket assignment
    val rddAssignment = leftRDDAssignment.union(rightRDDAssignment)
    val rddPartitioned = rddAssignment.partitionBy(new HashPartitioner(nBucket))
    
    // rddPartitioned.foreachPartition(partition => println("element in this partition: " + partition.length))
    
    // step 5, final join of local theta join in each partition
    val result = {
      rddPartitioned.mapPartitions(partitions => {
        val list = partitions.toList
        val left = list.filter(row => row._2._1 == "L").map(row => (row._1, row._2._2.getInt(index1))).iterator
        val right = list.filter(row => row._2._1 == "R").map(row => (row._1, row._2._2.getInt(index2))).iterator
        
        val joinResult = local_thetajoin(left, right, op)
        joinResult
      })
    }

    // result.foreach(row => println("element in this row: " + row))

    result
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

  def search(target: Int, l: List[Int]) = {
    def recursion(low:Int, high:Int): Int = (low + high)/2 match {
      case _ if high < low => (low + high)/2
      case mid if l(mid) > target => recursion(low, mid - 1)      
      case mid if l(mid) < target => recursion(mid + 1, high)
      case mid => (low + high)/2
    } 
    recursion(0, l.size - 1)
  }
}

