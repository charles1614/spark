#!/bin/bash

DEPS=/opt/deps
export PATH=$DEPS/pmix/bin:$DEPS/automake/bin:$DEPS/m4/bin:$DEPS/autoconf/bin:$DEPS/autotool/bin:$DEPS/libtool/bin:$PATH
export LD_LIBRARY_PATH=$DEPS/pmix/lib:$DEPS/libtool/lib:$LD_LIBRARY_PATH
export LIBRARY_PATH=$DEPS/pmix/lib:$DEPS/libtool/lib:$LIBRARY_PATH
export C_INCLUDE_PATH=$DEPS/pmix/include:$C_INCLUDE_PATH

