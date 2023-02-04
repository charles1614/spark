package org.apache.spark.mpi;

import org.apache.spark.blaze.deploy.mpi.NativeUtils;

public class NamespaceFinalize {
    public static void f() {
        String ns = NativeUtils.namespaceQuery();
        String[] ns_arr = ns.split(",");
        int ret = 2;
        for (String n : ns_arr) {
            if (n.equals("")) {
                ret = 1;
                break;
            }
            ret = NativeUtils.namespaceFinalize(n);
            System.out.println("Finalize Namespace " + n);
            break;
        }
    }

    public static void main(String[] args) {
        f();
    }
}
