#!/bin/zsh

# install jar to maven
mvn install:install-file -Dfile=/opt/ompi/lib/mpi.jar \
 -DgroupId=org.open-mpi \
 -DartifactId=java \
 -Dversion=5.0.1 \
 -Dpackaging=jar > ~/mvn_mpi

#compile native code
spark_home=${HOME}/git/spark/
cd ${spark_home}/blaze/src/main/native
cmake .
make
cp ./src/blaze/libblaze.so ${HOME}/lib
cp ./src/pmix/libmca_odls_blaze.so ${HOME}/lib/mca_odls_blaze.so

# compile spark with hadoop
cd ${spark_home}
mvn -Pnetlib-lgpl -Pyarn -Pkubernetes -Dhadoop.version=3.2.0 -DskipTests package
