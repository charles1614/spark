#!/bin/zsh

echo "get su permission"
sudo echo "done"                                                                                                                                                                                

# complie native lib
/usr/bin/cmake \
	--no-warn-unused-cli \
	-DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE \
	-DCMAKE_BUILD_TYPE:STRING=Debug \
	-DCMAKE_C_COMPILER:FILEPATH=/opt/rh/devtoolset-8/root/usr/bin/gcc \
	-H/home/xialb/opt/spark/blaze/src/main/native \
	-B/home/xialb/opt/spark/blaze/src/main/native/cmake-build-debug \
	-G "Unix Makefiles"

/usr/bin/cmake \
	--build /home/xialb/opt/spark/blaze/src/main/native/cmake-build-debug \
	--config Debug \
	--target all \
	-j 10 --
# move to home
cd $SPARK_HOME/blaze/src/main/native/
cp cmake-build-debug/src/blaze/libblaze.so ~/lib/libblaze.so
sudo mkdir $PRTE/lib/prte
sudo cp cmake-build-debug/src/blaze/libblaze.so $PRTE/lib/prte/libblaze.so
sudo cp cmake-build-debug/src/pmix/libmca_odls_blaze.so $PRTE/lib/prte/mca_odls_blaze.so
sudo cp cmake-build-debug/src/pmix/libmca_rmaps_blaze.so $PRTE/lib/prte/mca_rmaps_blaze.so
