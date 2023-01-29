#include "prun.h"

int main(int argc, char* argv[]){
    argv[0] = "/opt/deps/prrte/bin/prun";
    argv[1] = "--map-by";
    argv[2] = "rankfile:file=/tmp/rankfile";
    argv[3] = "-np";
    argv[4] = "2";
    argv[5] = "hostname";
    argv[6] = NULL;
    argc = 6;
    prun(argc, argv);
}
