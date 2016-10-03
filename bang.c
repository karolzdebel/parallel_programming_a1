#include "bang.h"

/*********************************************************************
 * FUNCTION NAME: partDataset
 * PURPOSE: Stores record data and collision indexes from a 
 *			specified partition of the file.
 * ARGUMENTS: . File to be read from.
 *		      . Position of the file to start reading from.
 *			  . How many records to read.
 * RETURNS: Address of Dataset containing all records found in the 
 *			portion provided.
 *********************************************************************/
static Dataset *partDataset(FILE *file, int startPos, int readCount);

/*********************************************************************
 * FUNCTION NAME: getRecord
 * PURPOSE: Stores record data from a line retrieved from data file.
 * ARGUMENTS: . Line of text from data file.
 * RETURNS: Record containing parsed data.
 *********************************************************************/
static Record *getRecord(char *line);

/*********************************************************************
 * FUNCTION NAME: sameCol
 * PURPOSE: Checks if two records are part of the same collion.
 * ARGUMENTS: . Record being compared.
 *			  . Record being compared.
 * RETURNS: True if the records are part of the same collision,
 *		    false otherwise.
 *********************************************************************/
static bool sameCol(char *rec1, char *rec2);

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

static int *startPositions(FILE *file, int workerCount){
	int *index = malloc(sizeof(int)*(workerCount-1));	//File position for each worker to start at
	int totalSize = fileSize(file);
	int sizeLeft = fileSize(file);
	int approxIndex = 0;
	int offset = 0, curWorker = 0;
	char curLine[SIZE_RECORD+1],prevLine[SIZE_RECORD+1];

	/*Account for header*/
	totalSize -= SIZE_HEADER+SIZE_EOL;
	sizeLeft -= SIZE_HEADER+SIZE_EOL;

	/*Worker 1 always starts at beginning*/
	index[0] = 0;

	/*Calculate where in the file each worker should start their job*/
	for (curWorker=0;curWorker<workerCount-1;curWorker++){
		
		approxIndex += sizeLeft/(workerCount-curWorker-1);	//Set to approximate index
		approxIndex += (SIZE_RECORD+SIZE_EOL)-(approxIndex%(SIZE_RECORD+SIZE_EOL));  //Align index with a record in file

		/*Seek to approximate index*/
		fseek(file,approxIndex+SIZE_HEADER+SIZE_EOL,SEEK_SET);

		/*Read first two lines*/
		fread((void*)prevLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
		prevLine[SIZE_RECORD] = '\0';
		fread((void*)curLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
		curLine[SIZE_RECORD] = '\0';

		/*Calculate how many records ahead of approximate index 
		are part of the same collision*/
		while (sameCol(prevLine,curLine)){
			offset++;
			strcpy(prevLine,curLine);
			fread((void*)curLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
			curLine[SIZE_RECORD] = '\0';
		}
		index[curWorker+1] = approxIndex+offset+SIZE_HEADER+SIZE_EOL;		//Set workers index to last record of a collision
		sizeLeft = totalSize-index[curWorker];				//Update the amount of work left
	}

	return index;
}

int main(int argc,char **argv){
	FILE *file;
	Dataset *dataset;
	int workerCount = strtol(argv[2],NULL,10);
	int *index;

	if (argc < 2){
		printf("Please provide csv file as argument.\n");
		return(EXIT_FAILURE);
	}

	file = fopen(argv[1],"r");
	index = startPositions(file,workerCount);

	dataset = partDataset(file,SIZE_HEADER+SIZE_EOL,countRecords(file));
	printf("Found %d collisions in %d records.\n",dataset->colNum,countRecords(file));
	
	fclose(file);

	return 0;
}

Dataset *getDataset(FILE *file, int workerCount){
	int *position;
	int length,readCount;
	Dataset (*dataset)[workerCount];	//Array of datasets

	/*Get the positions at which the works will be starting their parsing*/
	position = startPositions(file,workerCount);

	//for each worker
	for (int i=0;i<workerCount;i++){
		if (i != workerCount-1){
			length = position[i+1]-position[i];
		}
		else{
			length = fileSize(file)-position[i];
		}
		readCount = (position[i+1]-position[i]-SIZE_HEADER-SIZE_EOL)/SIZE_RECORD;
		partDataset(file,position[i],(position[i+1]-position[i]));
	}

	return NULL;
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
	return (strncmp(rec1,rec2,LENGTH_COLL) == 0);
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

Dataset *partDataset(FILE *file, int startPos, int readCount){
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
		/*Set collision to one on first read*/
		else{ dataset->colNum = 1; }

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
