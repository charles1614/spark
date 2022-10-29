
package org.apache.spark.examples.blaze.cg

import hdf.hdf5lib.H5
import hdf.hdf5lib.HDF5Constants

object HdfRead {
  def main(args: Array[String]): Unit = {
    val file = "/home/xialb/tmp/matrix_50000.h5"
    val dataset = "data"
    val dim_x, dim_y = 10000
    val rank = 2

//    val array = readDataSet(file, dataset, 20000, 20000)
    //    array.map(_.mkString(" ")).foreach(println)
//    println(array(0)(19))
  }

  def readDataSet(file: String, dataset: String, rows: Int, cols: Int,
    start: Array[Long]): Array[Array[Double]] = {
    val file_id = H5.H5Fopen(file, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT)
    val dataset_id = H5.H5Dopen(file_id, dataset, HDF5Constants.H5P_DEFAULT)
    val dset_data = Array.ofDim[Double](rows, cols)

    val filespace_id = H5.H5Dget_space(dataset_id)
    val memspace_id = H5.H5Screate_simple(2, Array(rows, cols), null)

    // select hyperslab
    //    val start = Array[Long](0, 0)
    val stride = Array[Long](1, 1)
    val count = Array[Long](rows, cols)
    val block = Array[Long](1, 1)

    println(start(0), start(1))
//    println(rows * cols)

    H5.H5Sselect_hyperslab(filespace_id, HDF5Constants.H5S_SELECT_SET, start, stride, count, block)
    //    H5.H5Sselect_hyperslab(memspace_id, HDF5Constants.H5S_SELECT_SET, start, stride, count, block)

    /* dataset_id, mem_type_id, mem_space_id, file_space_id, xfer, buffer */
    H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE,
      //      HDF5Constants.H5S_ALL, filespace_id,
      memspace_id, filespace_id,
      HDF5Constants.H5P_DEFAULT, dset_data)

//        println(dset_data(0)(0))


    if (dataset_id > 0) {
      H5.H5Dclose(dataset_id)
    }
    if (file_id > 0) {
      H5.H5Fclose(file_id)
    }
    dset_data
  }
}
