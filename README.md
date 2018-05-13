# Advanced Database System II
Project II of [Advanced Database System (CS-422)](http://isa.epfl.ch/imoniteur_ISAP/!itffichecours.htm?ww_i_matiere=1888826574&ww_x_anneeAcad=2017-2018&ww_i_section=249847&ww_i_niveau=&ww_c_langue=en) of [EPFL](https://www.epfl.ch/)

## Implementation of the following data processing frameworks over Spark:
1. Cube Operator.
2. Theta Join Operator (M-Bucket-I Algorithm).
3. Data Streaming Pipeline.

## Usage

### Prerequisites
1. Install [scala](https://www.scala-lang.org/).
2. Install scala build tool, [sbt](https://www.scala-sbt.org/).
3. Ensure your environment has recognized scala and sbt:
```bash
# Example
which scala
$ /usr/local/bin/scala
which sbt
/usr/local/bin/sbt
```
4. Read about [apache spark](https://spark.apache.org/) and [hadoop](http://hadoop.apache.org/), *the latter may not really be necessary to read*. 
5. Clone this repository.

### Building the java jar package
Go to `CS422-Project2` dir and follow the README there:
```bash
cd CS422-Project2
```

### Testing the applications over Spark environment
Go to `docker` dir and follow the README there:
```bash
cd docker
```

## License
MIT.