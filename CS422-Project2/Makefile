class?=thetajoin

package: 
	sbt package

compile:
	sbt "run-main $(class).Main"

test:
	sbt test

copy:
	cp target/scala-2.11/cs422-project2_2.11-0.1.0.jar ../docker/cs422-pr2/jars
