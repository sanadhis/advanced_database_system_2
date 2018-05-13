package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.math.ceil
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
    
    val rdd1JoinAttribute = rdd1.map(tuple => tuple.getInt(index1))
    val rdd2JoinAttribute = rdd2.map(tuple => tuple.getInt(index2))

    // step 1: sampling
    // row refer to rdd1, column refer to rdd2
    val overallRowSize = rdd1JoinAttribute.count().toInt
    val overalColumnSize = rdd2JoinAttribute.count().toInt
    val factor = sqrt(overallRowSize * overalColumnSize / reducers)

    val sizeOfRowSamples = ( ceil(overallRowSize / factor).toInt ) * 10 - 1 
    val sizeOfColumnSamples = ( ceil(overalColumnSize / factor).toInt ) * 10 - 1

    horizontalBoundaries = sampleData(sizeOfRowSamples, rdd1JoinAttribute)
    verticalBoundaries = sampleData(sizeOfColumnSamples, rdd2JoinAttribute)

    val horizontalBoundariesMod = 0 +: horizontalBoundaries :+ Int.MaxValue
    val verticalBoundariesMod = 0 +: verticalBoundaries :+ Int.MaxValue

    val horizontalCounts = (0 to horizontalBoundaries.size).toList.map( i =>
      rdd1JoinAttribute.filter(value => 
        (value >= horizontalBoundariesMod(i)) && (value < horizontalBoundariesMod(i+1))
      ).count().toInt
    )

    val verticalCounts = (0 to verticalBoundaries.size).toList.map( i =>
      rdd2JoinAttribute.filter(value => 
        (value >= verticalBoundariesMod(i)) && (value < verticalBoundariesMod(i+1))
      ).count().toInt
    )

    println("\nHorizontal boundaries: " + horizontalBoundaries)
    println("Vertical boundaries: " + verticalBoundaries)
    println("Horizontal count: " + horizontalCounts)
    println("Vertical count: " + verticalCounts)

    // implement histogram
    val histogram = {

      val mockHistogram = Array.fill(horizontalCounts.size){Array.fill(verticalCounts.size){0}}

      if (op == "!="){
        (0 until mockHistogram.size).foreach(x => {
          (0 until mockHistogram(x).size).foreach(y => {
            mockHistogram(x)(y) = 1
          })
        })
      }

      else{
        (horizontalBoundaries :+ Int.MaxValue).zipWithIndex.iterator.foreach( x => {
          (verticalBoundaries :+ Int.MaxValue).zipWithIndex.iterator.filter( y => 
            (x._1 >= verticalBoundariesMod(y._2) && x._1 < verticalBoundariesMod(y._2 + 1)) || (x._1 >= y._1 && horizontalBoundariesMod(x._2) < y._1) ).foreach(y => {
              // mark this area to be considered for join operation
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
      }
      
      mockHistogram
    }

    println("\nHistogram:")
    histogram.foreach(row => println(row.mkString("_")))
    println()

    // step 2: bucket assignment
    // iteratively try to find best bucket assignment
    val nRows = histogram.size
    val nColumns = histogram(0).size

    val bestAssignment = {
      val maxInput = bucketsize

      var lastReducerId = 1
      var lastRow = 0
      var lastColumn = 0
      var lastBestScore = Int.MinValue
      var totalRowCount = 0
      var totalColumnCount = 0
      
      val cumulatedAssignment = Array.fill(nRows){Array.fill(nColumns){0}}

      (0 until nRows).foreach(row => {
        val assignment = Array.fill(nRows){Array.fill(nColumns){0}}

        var totalAreaCount = List[Int]()
        var reducerId = lastReducerId

        totalRowCount += horizontalCounts(row)
        lastColumn = 0
        totalColumnCount = 0

        (0 until nColumns).foreach(column => {
                    
          totalColumnCount += verticalCounts(column)
          
          if(totalRowCount + totalColumnCount > maxInput){
            //marking
            (lastRow to row).foreach(r => {
              (lastColumn until column).foreach(c => {
                if(histogram(r)(c) == 1){
                  assignment(r)(c) = reducerId
                }
              })
            })

            totalAreaCount :+= totalRowCount * totalColumnCount
            totalColumnCount = verticalCounts(column)
            lastColumn = column
            reducerId += 1
          }
        
        })

        if(lastColumn < nColumns){
          (lastRow to row).foreach(r => {
            (lastColumn until nColumns).foreach(c => {
              if(histogram(r)(c) == 1){
                assignment(r)(c) = reducerId
              }
            })
          })
          totalAreaCount :+= totalRowCount * totalColumnCount
        }

        val score = totalAreaCount.sum / totalAreaCount.size
        println("score=" + score + " for row=" + (row+1).toString + " with area=" + totalAreaCount.size)
        if(score >= lastBestScore){
          lastBestScore = score
          (lastRow to row).foreach(r => {
            (0 until nColumns).foreach(c => {
              cumulatedAssignment(r)(c) = assignment(r)(c)
            })
          })
        }
        else{
          println("current score is lower! Selecting last best score of: best score=" + lastBestScore)
          lastReducerId = reducerId + 1
          lastRow = row
          totalRowCount = row
          lastBestScore = Int.MinValue
        }
      })

      if(lastBestScore == Int.MinValue){
        totalRowCount = 0
        totalColumnCount = 0
        (lastRow until nRows).foreach(r => {
          totalRowCount = horizontalCounts(r)
          (0 until nColumns).foreach(c => {
            totalColumnCount += verticalCounts(c)

            if(totalRowCount + totalColumnCount > maxInput){
              lastReducerId += 1
              totalColumnCount = verticalCounts(c)
            }

            if (histogram(r)(c) == 1){
              cumulatedAssignment(r)(c) = lastReducerId
            }
          })
        })
      }

      cumulatedAssignment
    }

    println("\nBest Assignment: ")
    bestAssignment.foreach(row => println(row.mkString("_")))
    println()

    val distinctValue = bestAssignment.flatMap(row => row).distinct.sorted.filter(id => id != 0 )
    val nBucket = distinctValue.size
    val reducerIdsLookup = distinctValue.zipWithIndex.map(id => id._1 -> (id._2+1) ).toMap

    // step 3, now assign value
    val leftAssignment = bestAssignment
    val rightAssignment = bestAssignment.transpose

    // left assignment
    val leftRDDAssignment = rdd1.flatMap(row => {
      val position = search(row.getInt(index1), horizontalBoundariesMod)
      leftAssignment(position).distinct.filter(assignment => assignment != 0).map( reducerId => (reducerIdsLookup(reducerId), ("L", row) ) )
    })

    // right assignment
    val rightRDDAssignment = rdd2.flatMap(row => {
      val position = search(row.getInt(index2), verticalBoundariesMod)
      rightAssignment(position).distinct.filter(assignment => assignment != 0).map( reducerId => (reducerIdsLookup(reducerId), ("R", row) ) )
    })

    // step 4, partition value based on bucket assignment
    val rddAssignment = leftRDDAssignment.union(rightRDDAssignment)
    val rddPartitioned = rddAssignment.partitionBy(new HashPartitioner(nBucket))
        
    // step 5, final join of local theta join in each partition
    val result = {
      rddPartitioned.mapPartitionsWithIndex( (index, partitions) => {
        val list = partitions.toList
        val left = list.filter(row => row._2._1 == "L").map(row => (row._1, row._2._2.getInt(index1))).iterator
        val right = list.filter(row => row._2._1 == "R").map(row => (row._1, row._2._2.getInt(index2))).iterator
        
        val joinResult = local_thetajoin(left, right, op)
        joinResult
      })
    }

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
      case "!=" => value1 != value2
    }
  }

  def sampleData(sampleSize: Int, joinAttribute: RDD[Int]): List[Int] = {
    var samples = Array.fill(sampleSize){0}

    for { i <- 0 until sampleSize } {
      var sampleValue = joinAttribute.takeSample(false, 1)(0)
      
      while (samples.contains(sampleValue)) {
        sampleValue = joinAttribute.takeSample(false, 1)(0)
      }

      samples(i) = sampleValue
    }

    quickSort(samples)
    samples.toList
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
