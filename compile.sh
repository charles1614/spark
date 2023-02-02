#!/bin/sh

mvnd  -pl :spark-core_2.12 -DskipTests -Dmaven.test.skip=true -Dmaven.compile.fork=true -Dscalastyle.skip=true -Dcheckstyle.skip package
rsync -a ~/git/spark/core/target/spark-core_2.12-3.2.1-SNAPSHOT.jar besh01:~/git/spark/assembly/target/scala-2.12/jars/spark-core_2.12-3.2.1-SNAPSHOT.jar
rsync -a ~/git/spark/core/target/spark-core_2.12-3.2.1-SNAPSHOT.jar localhost:~/git/spark/assembly/target/scala-2.12/jars/spark-core_2.12-3.2.1-SNAPSHOT.jar

