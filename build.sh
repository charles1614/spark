#!/bin/zsh

# install jar to maven
mvn install:install-file -Dfile=/opt/ompi/lib/mpi.jar \
  -DgroupId=org.open-mpi \
  -DartifactId=java \
  -Dversion=5.0.1 \
  -Dpackaging=jar > ~/mvn_mpi

# mvn install:install-file -Dfile=${HOME}/tmp/hdf5-1.12.1/java/src/jarhdf5-1.12.1.jar \
#   -DgroupId=org.hdfgroup \
#   -DartifactId=hdf5 \
#   -Dversion=1.12.1 \
#   -Dpackaging=jar

#compile native code
spark_home=${HOME}/git/spark/
cd ${spark_home}/blaze/src/main/native
rm -rf build
cmake -S . -B build
cmake --build build
cp ./src/blaze/libblaze.so ${spark_home}/lib/
sudo mkdir /opt/deps/prrte/lib/prte
sudo cp ./build/src/pmix/libmca_odls_blaze.so /opt/deps/prrte/lib/prte/mca_odls_blaze.so

# compile spark with hadoop
cd ${spark_home}
export MAVEN_OPTS="-Xss64m -Xmx4g -XX:ReservedCodeCacheSize=2g"
mvn -Pnetlib-lgpl -Pyarn -Pkubernetes -Dhadoop.version=3.3.0 -Dscalastyle.skip=true -DskipTests install 
