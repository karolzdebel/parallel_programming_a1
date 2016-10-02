#include "bang.h"

int main(int argc,char **argv){
	FILE *file;

	if (argc < 2){
		printf("Please provide csv file as argument.\n");
		return(EXIT_FAILURE);
	}

	file = fopen(argv[1],"r");
	getDataset(file,SIZE_HEADER,SIZE_RECORD*100);
	return 0;
}
Record *getRecord(char *line){
	return NULL;
}

bool sameCol(Record rec1, Record rec2){
	return false;
}

static int recCount(Dataset *dataset){
	return dataset->recNum;
}

static int colCount(Dataset *dataset){
	return dataset->colNum;
}


static void addRecord(Dataset *dataset, Record *record){
	Record **arr = dataset->records;

	dataset->recNum++;
	
	/*Allocate more memory for new record*/
	if (arr != NULL){
		arr = realloc(arr,sizeof(Record*)*recNum(dataset));
		arr[recNum-1] = record;
	}
	else{
		arr = malloc(sizeof(Record*));
		arr[0] = record;
	}
}

static void addIndex(Dataset *dataset, int index){
	int *arr = dataset->collisionIndex;

	if (arr != NULL){
		arr = realloc(arr,sizeof(int)*colCount(dataset));
		arr[colNum-1] = index;
	}
	else{
		arr = malloc(sizeof(int));
		arr[0] = index;
	}
}

static void createDataset(Dataset **dataset){
	*dataset = malloc(sizeof(Dataset));
	(*dataset)->colNum = 0;
	(*dataset)->recNum = 0;
	(*dataset)->collisionIndex = NULL;
	(*dataset)->records = NULL; 
}

Dataset *getDataset(FILE *file, int startPos, int length){
	Dataset *dataset = NULL;
	Record *prevRecord = NULL;
	Record *curRecord = NULL;
	char line[SIZE_RECORD+1];
	int readCount = length/SIZE_RECORD;		//Calculate neccessary amount of reads

	createDataset(&dataset);

	/*Go to provided address in file*/
	fseek(file,startPos,0);

	/*Read line from data file provided*/
	for(int i=0; i<readCount; i++){

		fread((void*)line,sizeof(char),SIZE_RECORD+1,file);
		printf("%s",line);
		/*Add record to dataset*/
		addRec(dataset);

		/*Store previous record for later comparison*/
		prevRecord = curRecord;
		curRecord = getRecord(line);

		/*Don't compare if the first record is being parsed*/
		if (prevRecord != NULL){
			
			/*Compare record with previous to see if it is part of the same collision*/
			if (!sameCol(*prevRecord,*curRecord)){

				/*If not part of the same collision than create new index*/
				dataset->colNum++;
				addIndex(dataset, colNum(dataset), recNum(dataset));
			}
		}
	}  

	return dataset;
}
