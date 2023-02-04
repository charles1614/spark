#
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
#

import sys
import time
from random import random
from operator import add 
import numpy as np 
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
            .builder\
            .appName("PythonPi")\
            .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 4

    base = "/home/xialb/git/lqcdworkflow/tests/01.hmc"
    input = base + "/input/hmc.prec_wilson.ini.xml"
    output = base + "/output/hmc.prec_wilson.out.xml"
    hmclog = base + "/output/hmc.prec_wilson.log.xml"

    list = np.arange(0, 3, 1)

    rdd = spark.sparkContext\
            .parallelize(np.arange(0, 4, 1), partitions)\
            .mpipipe('/home/xialb/git/ompi/examples/hello_c')

    rdd.foreach(lambda x: print(x))
    rdd1 = rdd.mpipipe('/home/xialb/git/ompi/examples/hello_c')
    rdd1.foreach(lambda x: print(x))

    spark.stop()
