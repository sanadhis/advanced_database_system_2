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
    val relatedAttributes = rdd.map(row => 
      if (agg == "COUNT")  ( (index.map(i => row(i) )).mkString("-"), 1.toDouble )
      else if (agg == "AVG")  ( (index.map(i => row(i) )).mkString("-"), row.getInt(indexAgg).toDouble ) 
      else  ( (index.map(i => row(i) )).mkString("-"), row.getInt(indexAgg).toDouble ) 
    )

    val reducedBottomCell = 
      if (agg == "COUNT") relatedAttributes.repartition(reducers).reduceByKey(sum)
      else if (agg == "MAX") relatedAttributes.repartition(reducers).reduceByKey(max)
      else if (agg == "MIN") relatedAttributes.repartition(reducers).reduceByKey(min)
      else if (agg == "AVG") relatedAttributes.repartition(reducers).reduceByKey(sum)
      else relatedAttributes.repartition(reducers).reduceByKey(sum)

    // begin phase 2 of MRDataCube
    val partialUpperCell  = reducedBottomCell.flatMap(
      row => (0 to groupingAttributes.length).toList.flatMap( 
        n => row._1.split("-").combinations(n).toList.map(
          part => if (agg!="AVG") (part.mkString("-"), row._2) else (part.mkString("-"), row._2) ) 
          )
        )
    
    val reducerFinal = 
      if (agg == "COUNT") partialUpperCell.repartition(reducers).reduceByKey(sum)
      else if (agg == "MAX") partialUpperCell.repartition(reducers).reduceByKey(max)
      else if (agg == "MIN") partialUpperCell.repartition(reducers).reduceByKey(min)
      else if (agg == "AVG") partialUpperCell.repartition(reducers).reduceByKey(sum)
      else partialUpperCell.repartition(reducers).reduceByKey(sum)

    reducerFinal
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

}
