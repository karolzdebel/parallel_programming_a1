#include "bang.h"

static int fileSize(FILE *file){
	int size;

	fseek(file,0,SEEK_END);
	size = ftell(file);
	rewind(file);

	return size;
}

static int countRecords(FILE *file){
	return ( (fileSize(file)-(SIZE_HEADER+SIZE_EOL) ) / (SIZE_RECORD+SIZE_EOL) );
}

int main(int argc,char **argv){
	FILE *file;
	Dataset *dataset;

	if (argc < 2){
		printf("Please provide csv file as argument.\n");
		return(EXIT_FAILURE);
	}

	file = fopen(argv[1],"r");
	dataset = getDataset(file,SIZE_HEADER+SIZE_EOL,countRecords(file));
	printf("Found %d collisions in %d records.\n",dataset->colNum,countRecords(file));
	return 0;
}

static void createRecord(Record **record){
	*record = malloc(sizeof(Record));
	(*record)->date.year = 0;
	(*record)->date.month = 0;
	(*record)->date.day = 0;
	(*record)->gender = ' ';
	(*record)->vehYear = 0;
	(*record)->death = false;
}

static void printRecord(Record record){
	printf("RECORD INFO - Year:%d\tMonth:%d\tDay:%d\tGender:%c\tVehicle Year:%d\t"
		,record.date.year,record.date.month,record.date.day,record.gender,record.vehYear);

	if (record.death){ printf("Fatality: Yes\n"); }
	else{ printf("Fatality: No\n"); }
}

Record *getRecord(char *recLine){
	int col;		//column
	char *token = NULL;
	char line[SIZE_RECORD+1];
	Record *record;

	createRecord(&record);

	strcpy(line,recLine);
	token = strtok(line,",");

	//Go through every column and store data if it is needed
	for (col=1; col<COL_COUNT+1; col++){
		

		switch(col){
			case COL_CYEAR:
				record->date.year = strtol(token,NULL,10);
				break;

			case COL_MNTH:
				record->date.month = strtol(token,NULL,10);
				break;

			case COL_DAY:
				record->date.day = strtol(token,NULL,10);
				break;

			case COL_SEV:
				record->death = (token[0] == '1');
				break;

			case COL_LOC:
				strcpy(record->location,token);
				break;

			case COL_VYEAR:
				record->vehYear = strtol(token,NULL,10); //strtol returns 0 to vehyear if year is not provided in csv
				break;

			case COL_SEX:
				record->gender = token[0];
				break;
		}

		/*Tokenize the next column*/
		token = strtok(NULL,",");

	}

	return record;
}

bool sameCol(char *rec1, char *rec2){
	return strncmp(rec1,rec2,LENGTH_COLL);
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
		arr = realloc(arr,sizeof(Record*)*recCount(dataset));
		arr[recCount(dataset)-1] = record;
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
		arr[colCount(dataset)-1] = index;
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

Dataset *getDataset(FILE *file, int startPos, int readCount){
	Dataset *dataset = NULL;
	Record *record = NULL;
	char line[SIZE_RECORD+SIZE_EOL+1], prevLine[SIZE_RECORD+SIZE_EOL+1];

	createDataset(&dataset);

	/*Go to provided address in file*/
	fseek(file,startPos,SEEK_SET);

	/*Read line from data file provided*/
	for(int i=0; i<readCount; i++){

		/*Store previous line for later collision comparison
		 unless its the first line being read*/
		if (i > 0){
			strcpy(prevLine,line);
		}

		/*Read next line of record data*/
		fread((void*)line,1,SIZE_RECORD+SIZE_EOL,file);
		line[SIZE_RECORD] = '\0';

		/*Get the record and add to dataset*/
		dataset->recNum++;
		record = getRecord(line);
		addRecord(dataset,record);

		/*Compare previous and current line read for same collision
		unless its the first line being read*/
		if (i > 0){
			
			if (!sameCol(prevLine,line)){
				/*If not part of the same collision than create new index*/
				dataset->colNum++;
				addIndex(dataset, recCount(dataset));
			}
		}
	}  
	return dataset;
}
