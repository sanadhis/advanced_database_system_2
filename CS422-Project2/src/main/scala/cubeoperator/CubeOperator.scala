package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // begin phase 1 of MRDataCube
    val relatedAttributes: RDD[Any] = rdd.map(row => 
      if (agg == "COUNT")  ( (index.map(i => row(i) )).mkString("-"), 1.toDouble )
      else if (agg == "AVG")  ( (index.map(i => row(i) )).mkString("-"), (row.getInt(indexAgg).toDouble, 1.toDouble) ) 
      else  ( (index.map(i => row(i) )).mkString("-"), row.getInt(indexAgg).toDouble ) 
    )

    val reducedBottomCell = agg match {
      case "AVG" => relatedAttributes.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(averageFunction, reducers)
      case _     => relatedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(aggFunction(agg), reducers)
    }

    // begin phase 2 of MRDataCube
    val partialUpperCell  = reducedBottomCell.flatMap(
      row => (0 to groupingAttributes.length).toList.flatMap( 
        n => row._1.split("-").combinations(n).toList.map(
          part => (part.mkString("-"), row._2)
          )
        )
    )

    val reducerFinal: RDD[(String, Double)] = agg match {
      case "AVG" => partialUpperCell.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(averageFunction, reducers).mapValues{ case (sum , count) => sum / count}
      case _     => partialUpperCell.asInstanceOf[RDD[(String, Double)]].reduceByKey(aggFunction(agg), reducers)
    }

    reducerFinal
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // map
    val mappedAttributes: RDD[Any] = rdd.flatMap(row => 
        (0 to index.length).toList.flatMap(
          n => index.map(i => row(i) ).combinations(n).toList.map(
            part => if (agg=="COUNT") (part.mkString("-"), 1.toDouble) 
                    else if (agg=="AVG") (part.mkString("-"), (row.getInt(indexAgg).toDouble, 1.toDouble) ) 
                    else (part.mkString("-"), row.getInt(indexAgg).toDouble )
        )
      )
    )

    val reducedAttributes: RDD[(String, Double)] = agg match {
      case "AVG" => mappedAttributes.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(averageFunction, reducers).mapValues{ case (sum , count) => sum / count}
      case _     => mappedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(aggFunction(agg), reducers)
    }

    reducedAttributes
  }

  // aggregate functions here
  def averageFunction : ((Double, Double), (Double, Double)) => (Double, Double) = {
    (left: (Double, Double), right: (Double, Double)) => (left._1 + right._1, left._2 + right._2)
  }

  def aggFunction(agg: String) : (Double, Double) => Double = agg match {
      case "MIN" => (value1: Double, value2: Double) => if (value1 < value2) value1 else value2
      case "MAX" => (value1: Double, value2: Double) => if (value1 > value2) value1 else value2
      case _     => (value1: Double, value2: Double) => value1 + value2
  }

}
