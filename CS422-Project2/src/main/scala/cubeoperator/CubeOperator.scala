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

    // aggregate functions here
    val sum = (value1: Double, value2: Double) => value1 + value2
    val max = (value1: Double, value2: Double) => if (value1 > value2) value1 else value2
    val min = (value1: Double, value2: Double) => if (value1 < value2) value1 else value2
    val avg = (left: (Double, Double), right: (Double, Double)) => (left._1 + right._1, left._2 + right._2)

    // begin phase 1 of MRDataCube
    val relatedAttributes: RDD[Any] = rdd.map(row => 
      if (agg == "COUNT")  ( (index.map(i => row(i) )).mkString("-"), 1.toDouble )
      else if (agg == "AVG")  ( (index.map(i => row(i) )).mkString("-"), (row.getInt(indexAgg).toDouble, 1.toDouble) ) 
      else  ( (index.map(i => row(i) )).mkString("-"), row.getInt(indexAgg).toDouble ) 
    )

    val reducedBottomCell = 
      if (agg == "COUNT") relatedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)
      else if (agg == "MAX") relatedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(max, reducers)
      else if (agg == "MIN") relatedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(min, reducers)
      else if (agg == "AVG") relatedAttributes.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(avg, reducers)
      else relatedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)

    // begin phase 2 of MRDataCube
    val partialUpperCell  = reducedBottomCell.flatMap(
      row => (0 to groupingAttributes.length).toList.flatMap( 
        n => row._1.split("-").combinations(n).toList.map(
          part => (part.mkString("-"), row._2)
          )
        )
    )

    val reducerFinal: RDD[(String, Double)] = 
      if (agg == "COUNT") partialUpperCell.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)
      else if (agg == "MAX") partialUpperCell.asInstanceOf[RDD[(String, Double)]].reduceByKey(max, reducers)
      else if (agg == "MIN") partialUpperCell.asInstanceOf[RDD[(String, Double)]].reduceByKey(min, reducers)
      else if (agg == "AVG") partialUpperCell.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(avg, reducers).mapValues{ case (sum , count) => sum / count}
      else partialUpperCell.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)

    reducerFinal
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // aggregate functions here
    val sum = (value1: Double, value2: Double) => value1 + value2
    val max = (value1: Double, value2: Double) => if (value1 > value2) value1 else value2
    val min = (value1: Double, value2: Double) => if (value1 < value2) value1 else value2
    val avg = (left: (Double, Double), right: (Double, Double)) => (left._1 + right._1, left._2 + right._2)

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

    val reducedAttributes: RDD[(String, Double)] = 
      if (agg == "COUNT") mappedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)
      else if (agg == "MAX") mappedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(max, reducers)
      else if (agg == "MIN") mappedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(min, reducers)
      else if (agg == "AVG") mappedAttributes.asInstanceOf[RDD[(String, (Double, Double) )]].reduceByKey(avg, reducers).mapValues{ case (sum , count) => sum / count}
      else mappedAttributes.asInstanceOf[RDD[(String, Double)]].reduceByKey(sum, reducers)

    reducedAttributes
  }

}
