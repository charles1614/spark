#include "prte.h"

int main(int argc, char* argv[]){
    argv[0] = "/opt/deps/prrte/bin/prte";
    argv[1] = "-H";
    argv[2] = "besh01:12";
    // argv[6] = NULL;
    argc = 3;
    prte(argc, argv);
}
