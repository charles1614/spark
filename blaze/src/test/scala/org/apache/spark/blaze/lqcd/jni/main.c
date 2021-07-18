//
// Created by xia on 10/29/20.
//
#include <jni.h>
#include <stdlib.h>
#include <init.h>
#include <linalg.h>
#include <dirac.h>
#include <test.h>

int main(){
    char str[] = "/home/xia/CLionProjects/simpleLQCD/main/test.8.cfg";
    FILE* ptr = fopen(str, "rb");
	if(ptr == NULL){
		printf("Error in read configurations\n");
		exit(1);
	}
	su3_field *u;
    su3_field_alloc(&u);
	fread(u, sizeof(su3_field), 1, ptr);
	fclose(ptr);
    printf("gauge field readed: \n");
    printf("%f\n", (((*u)[0][0][0][0][0]).c11.re));
	su3_field_free(&u);
}

