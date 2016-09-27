#include "bang.h"

int main(int argc,char **argv){

	if (argc < 2){
		printf("Please provide csv file as argument.\n");
		return(EXIT_FAILURE);
	}
	printf("%s\n",argv[1]);
	return 0;
}