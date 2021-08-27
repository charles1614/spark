package org.apache.spark.blaze.deploy.mpi;


import org.apache.spark.blaze.ompi.Peer;

public class MPIContextNative {
    public static native String getNamespace();
    public static native Integer getRanks();
    public static native Peer getSrvPeer();
}
