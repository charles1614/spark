#include "prun.h"

int main(int argc, char* argv[]){
    argv[0] = "/opt/deps/prrte/bin/prun";
    // argv[1] = "--map-by";
    // argv[2] = "rankfile:file=/tmp/rankfile";
    argv[1] = "-np";
    argv[2] = "4";
    argv[3] = "hostname";
    argv[4] = NULL;
    argc = 4;
    prun(argc, argv);
}
