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

    val sizeOfRowSamples = ( ceil(overallRowSize / factor).toInt )
    val sizeOfColumnSamples = ( ceil(overalColumnSize / factor).toInt )
    
    // random samples for each relation
    val horizontalBoundaries = sampleData(sizeOfRowSamples, rdd1JoinAttribute)
    val verticalBoundaries = sampleData(sizeOfColumnSamples, rdd2JoinAttribute)

    val horizontalBoundariesMod = 0 +: horizontalBoundaries :+ Int.MaxValue
    val verticalBoundariesMod = 0 +: verticalBoundaries :+ Int.MaxValue

    // number of values that fall in each partition of rdd1
    val horizontalCounts = (0 to horizontalBoundaries.size).toList.map( i =>
      rdd1JoinAttribute.filter(value => 
        (value >= horizontalBoundariesMod(i)) && (value < horizontalBoundariesMod(i+1))
      ).count().toInt
    )

    // number of values that fall in each partition of rdd2
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

      // mark all if it is a not equal operation
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

              // mark upper right triangle of the histogram for "<" and "<=" operators
              if(op == "<" || op == "<="){
                (y._2 until verticalCounts.size).foreach(yPos => {
                  mockHistogram(x._2)(yPos) = 1
                })
              }
              // mark lower left triangle of the histogram for ">" and ">=" operators              
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
    // iteratively try to find best buckets assignment
    val nRows = histogram.size
    val nColumns = histogram(0).size

    val bestAssignment = {

      // map the actual row and column position into histogram position based on horizontal/vertical counts
      // this is useful in order to find which row/colum interval is marked as "interesting" area
      def findPosition(index: Int, counts: List[Int]): Int = {

        var total = 0     // total counts so far
        var position = 0  // position relative to histogram
        
        // loop while index offset still greater than total counts
        counts.iterator.takeWhile(count => total + count <= index ).zipWithIndex.foreach(count => {
          total += count._1
          position = count._2 + 1
        })

        position
      }

      // final assignment for each row and column table based on scoring calculation of the M-Bucket-I Algorithm
      val finalAssignment = Array.fill(overallRowSize){Array.fill(overalColumnSize){0}}
      val maxInput = bucketsize     // to limit input of each bucket
      val maxOutput = bucketsize    // to limit output of each bucket

      // control variable to bucket assignment
      var lastBucketId = 1
      var lastRow = 0                   // starting row index of a bucket
      var lastColumn = 0                // starting column index of a bucket
      var totalRowCount = 0             // total row size of a bucket
      var totalColumnCount = 0          // total column size of a bucket
      var lastBestScore = Int.MinValue  // keep the best score of M-Bucket-I algorithm
      
      (0 until overallRowSize).foreach(row => {
        // temporary assignment, the elements will be changed every row iteration
        val assignment = Array.fill(overallRowSize){Array.fill(overalColumnSize){0}}

        // total area covered by all n bucket in this row iteration
        var totalAreaCount = List[Int]()

        // ensure every row iteration start with different bucket id 
        // (avoid continuing bucket id of previous row's last bucket)
        var bucketId = lastBucketId

        // keep track of the row size for bucket(s) in every row iteration
        totalRowCount = row - lastRow + 1

        // reset lastColumn and total number of column every row iteration
        totalColumnCount = 0
        lastColumn = 0

        (0 until overalColumnSize).foreach(column => {
                    
          // keep track of the column size for bucket(s) every column iteration
          totalColumnCount = column - lastColumn + 1
          
          // if exceed maxInput or maxOutput, add bucket (increase bucket id), 
          // store totalAreaCount of this bucket, reset totalColumnCount and lastColumn
          if(totalRowCount + totalColumnCount > maxInput || totalRowCount * totalColumnCount > maxOutput){
            // println("cannot handle: " + totalRowCount + "," +  totalColumnCount)
            totalAreaCount :+= totalRowCount * (totalColumnCount - 1)
            lastColumn = column
            totalColumnCount = 1
            bucketId += 1
          }


          // map actual column index, relative to histogram "marking" based on vertical counts          
          val columnPosition = findPosition(column, verticalCounts)

          (lastRow to row).foreach(r => {
            // map actual row index, relative to histogram "marking" based on horizontal counts
            val rowPosition = findPosition(r, horizontalCounts)
            
            // assign if and only if the cell (row, column) falls into an "interesting" area
            // which is already marked in histogram
            if(histogram(rowPosition)(columnPosition) == 1){
              // mark the temporary assignment in every column iteration
              assignment(r)(column) = bucketId
            }

          })

        })

        // add the last bucket's area count (relative to this row iteration)
        totalAreaCount :+= totalRowCount * totalColumnCount

        // calculate and print score
        val score = totalAreaCount.sum / totalAreaCount.size
        
        // // uncomment to see score every row iteration
        // println("score=" + score + " for row=" + (row+1).toString + " with area=" + totalAreaCount.size)

        // update the final assignment if score is increasing
        if(score >= lastBestScore){
          lastBestScore = score
          (lastRow to row).foreach(r => {
            (0 until overalColumnSize).foreach(c => {
              finalAssignment(r)(c) = assignment(r)(c)
            })
          })
        }
        // else, reset the checkpoint (relative lastRow, etc) and increase last bucket id
        else{
          // println("current score is lower! Selecting last best score of: best score=" + lastBestScore)
          lastBucketId = bucketId + 1
          lastRow = row
          totalRowCount = row
          lastBestScore = Int.MinValue
        }

      })

      // if the reset (lower score) happens in the last row, iterate through the last row
      // to assign the area in this row into bucket(s)
      if(lastBestScore == Int.MinValue){
        totalRowCount = 0
        totalColumnCount = 0
        lastColumn = 0

        (lastRow until overallRowSize).foreach(row => {

          totalRowCount = row - lastRow + 1

          totalColumnCount = 0

          // map actual row index, relative to histogram "marking" based on horizontal counts
          val rowPosition = findPosition(row, horizontalCounts)
          
          (0 until overalColumnSize).foreach(column => {
            totalColumnCount += column - lastColumn + 1

            // increase bucket id if total input (row + column) exceed maxInput or total output (row * column) exceed maxOutput
            if(totalRowCount + totalColumnCount > maxInput || totalRowCount * totalColumnCount > maxOutput){
              lastBucketId += 1
              totalColumnCount = 1
              lastColumn = column
            }

            // map actual column index, relative to histogram "marking" based on vertical counts
            val columnPosition = findPosition(column, verticalCounts)

            // assign if and only if the cell (row, column) falls into an "interesting" area
            if(histogram(rowPosition)(columnPosition) == 1){
              // mark and assign area into bucket
              finalAssignment(rowPosition)(columnPosition) = lastBucketId
            }

          })

        })
        
      }

      finalAssignment
    }

    // // uncomment to see final assignment (will print big matrix!)
    // println("\nBest Assignment: ")
    // bestAssignment.foreach(row => println(row.mkString("_")))
    // println()

    // build a lookup map to resolve bucket id assigment into actual reducer id assignment
    // e.g. bucket id: 1,3,5,8 -> reducer id: 1,2,3,4
    val distinctValue = bestAssignment.flatMap(row => row).distinct.sorted.filter(id => id != 0 )
    val reducerIdsLookup = distinctValue.zipWithIndex.map(id => id._1 -> (id._2+1) ).toMap

    // the number of bucket is equal to the number of reducer needed for the algorithm
    val nBucket = distinctValue.size

    // step 3, now assign value
    val leftAssignment = bestAssignment
    val rightAssignment = bestAssignment.transpose

    // sort the data before final bucket & reducer assignment
    val sortedRow = rdd1JoinAttribute.sortBy(value => value)
    val sortedColumn = rdd2JoinAttribute.sortBy(value => value)

    // left "L" assignment
    val leftRDDAssignment = sortedRow.zipWithIndex.flatMap(row => {
      val value = row._1
      val position = row._2.toInt
      leftAssignment(position).distinct.filter(assignment => assignment != 0).map( bucketId => 
        (reducerIdsLookup(bucketId), ("L", value) ) 
      )
    })

    // right "R" assignment
    val rightRDDAssignment = sortedColumn.zipWithIndex.flatMap(row => {
      val value = row._1
      val position = row._2.toInt
      rightAssignment(position).distinct.filter(assignment => assignment != 0).map( bucketId => 
        (reducerIdsLookup(bucketId), ("R", value) ) 
      )
    })

    // step 4, partition value based on bucket assignment
    val rddAssignment = leftRDDAssignment.union(rightRDDAssignment)
    val rddPartitioned = rddAssignment.partitionBy(new HashPartitioner(nBucket))
        
    // step 5, final join of local theta join in each partition
    val result = {
      rddPartitioned.mapPartitionsWithIndex( (index, partitions) => {
        val list = partitions.toList
        val left = list.filter(row => row._2._1 == "L").map(row => (row._1, row._2._2)).iterator
        val right = list.filter(row => row._2._1 == "R").map(row => (row._1, row._2._2)).iterator
        
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
    val dat2List = dat2.toList
        
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

  /*
   * sample data from a RDD, ensure there is no duplication in the samples
   * return the sorted data as a list
   * */
  def sampleData(sampleSize: Int, joinAttribute: RDD[Int]): List[Int] = {
    val samples = joinAttribute.distinct.takeSample(false, sampleSize)

    quickSort(samples)
    samples.toList
  }

  /*
   * binary search
   * */    
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
