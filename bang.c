#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

#define SIZE_RECORD 61			//Length of a record
#define SIZE_HEADER 145			//Lenght of a header
#define SIZE_EOL 2				//Amount of EOL characters in each line
#define LENGTH_COLL 32			//Length of characters representing collision level data
#define COL_COUNT 22			//Amount of columns found in each record
#define COL_CYEAR 1				//Collision year
#define COL_MNTH 2				//Collision month
#define COL_DAY 3				//Collision day
#define COL_SEV 5				//Collision severity
#define COL_LOC 8				//Collision location
#define COL_VYEAR 15			//Vehicle year involved in collision
#define COL_SEX 17				//Gender of individual involved in collision

typedef struct Date Date;
typedef struct Date {
	int year;					//Year collision occured
	int month;					//Month collision occured ranging 1-12, -1 for unspecified
	int day;					//Day collision occured ranging 1-7, -1 for unspecified
} Date;

typedef struct Record Record;
typedef struct Record {
	Date date;					//Date the collision occured
	char location[2];			//Code of accident location 1-12 and QQ
	char gender;				//Gender of individual involved
	int vehYear;				//Vehicle year involved in collision
	bool death;					//Whether the person involved in the collision died
} Record;

typedef struct Dataset Dataset;
typedef struct Dataset {
	int colNum;					//Number of collisions found in dataset
	int recNum;					//Number of records found in dataset
	int *collisionIndex;		//Array storing indexes of each collsion
	Record **records;			//Array of records addresses
} Dataset;


/******************HELPER FUNCTION DOCUMENTATION*********************/
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

/*********************************************************************
 * FUNCTION NAME: fileSize
 * PURPOSE: Gets size of file in bytes.
 * ARGUMENTS: . File to find size of.
 * RETURNS: Integer containg size of file in bytes.
 *********************************************************************/
static int fileSize(FILE *file);

/*********************************************************************
 * FUNCTION NAME: countRecords
 * PURPOSE: Calculates the number of records in a file.
 * ARGUMENTS: . File which contains the records.
 * RETURNS: Integer containing the number of records.
 *********************************************************************/
static int countRecords(FILE *file);

/*********************************************************************
 * FUNCTION NAME: startPositions
 * PURPOSE: Gets the indexes of the file position each worker should
 *			start at.
 * ARGUMENTS: . File being read by workers.
 *			  . Number of workers performing the job.
 * RETURNS: Integer array of size workerCount containg the indexes 
 *			for each worker.
 *********************************************************************/
static int *startPositions(FILE *file, int workerCount);

/*********************************************************************
 * FUNCTION NAME: createRecord
 * PURPOSE: Allocates memory to and initializes the record.
 * ARGUMENTS: . Address of the pointer to the address of the record.
 *********************************************************************/
static void createRecord(Record **record);

/*********************************************************************
 * FUNCTION NAME: printRecord
 * PURPOSE: Debugging function used to print record data.
 * ARGUMENTS: . Record to be read.
 *********************************************************************/
static void printRecord(Record record);

/*********************************************************************
 * FUNCTION NAME: recCount
 * PURPOSE: Gets the number of records found in the dataset
 *			data structure.
 * ARGUMENTS: . Dataset data structure address.
 * RETURNS: Integer containg the number of records found.
 *********************************************************************/
static int recCount(Dataset *dataset);

/*********************************************************************
 * FUNCTION NAME: colCount
 * PURPOSE: Gets the number of collision found in the dataset
 *			data structure.
 * ARGUMENTS: . Dataset data structure address.
 * RETURNS: Integer containg the number of collisions.
 *********************************************************************/
static int colCount(Dataset *dataset);

/*********************************************************************
 * FUNCTION NAME: addRecord
 * PURPOSE: Add record to the dataset data structure.
 * ARGUMENTS: . Dataset to add to.
 *			  . Record being added.
 *********************************************************************/
static void addRecord(Dataset *dataset, Record *record);

/*********************************************************************
 * FUNCTION NAME: addIndex
 * PURPOSE: Adds index of a collision to the dataset data structure.
 * ARGUMENTS: . Dataset collision took part in.
 *			  . Index of collision.
 *********************************************************************/
static void addIndex(Dataset *dataset, int index);

/*********************************************************************
 * FUNCTION NAME: createDataset
 * PURPOSE: Allocate memory and initialize dataset.
 * ARGUMENTS: . Address of the pointer to the dataset.
 *********************************************************************/
static void createDataset(Dataset **dataset);
/*********************************************************************/

static bool sameCol(char *rec1, char *rec2){
	return (strncmp(rec1,rec2,LENGTH_COLL) == 0);
}

static int recCount(Dataset *dataset){
	return dataset->recNum;
}

static int colCount(Dataset *dataset){
	return dataset->colNum;
}

static int fileSize(FILE *file){
	int size;

	fseek(file,0,SEEK_END);
	size = ftell(file);
	rewind(file);

	return size;
}

static void printRecord(Record record){
	printf("RECORD INFO - Year:%d\tMonth:%d\tDay:%d\tGender:%c\tVehicle Year:%d\t"
		,record.date.year,record.date.month,record.date.day,record.gender,record.vehYear);

	if (record.death){ printf("Fatality: Yes\n"); }
	else{ printf("Fatality: No\n"); }
}

static int countRecords(FILE *file){
	return ( (fileSize(file)-(SIZE_HEADER+SIZE_EOL) ) / (SIZE_RECORD+SIZE_EOL) );
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

static void createDataset(Dataset **dataset){
	*dataset = malloc(sizeof(Dataset));
	(*dataset)->colNum = 0;
	(*dataset)->recNum = 0;
	(*dataset)->collisionIndex = NULL;
	(*dataset)->records = NULL; 
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

static int *startPositions(FILE *file, int workerCount){
	int *index = malloc(sizeof(int)*workerCount);	//File position for each worker to start at
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

static Record *getRecord(char *recLine){
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

static Dataset *partDataset(FILE *file, int startPos, int readLength){
	Dataset *dataset = NULL;
	Record *record = NULL;
	int i,readCount;
	char line[SIZE_RECORD+SIZE_EOL+1], prevLine[SIZE_RECORD+SIZE_EOL+1];

	createDataset(&dataset);

	/*Go to provided address in file*/
	fseek(file,startPos,SEEK_SET);

	/*Calculate number of reads based on length provided*/
	readCount = readLength/(SIZE_RECORD+SIZE_EOL);

	/*Read line from data file provided*/
	for(i=0; i<readCount; i++){

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

int main(int argc,char **argv){
        FILE *file;
        int *position;
		int i,workerCount,length;

		workerCount = strtol(argv[2],NULL,10);

		file = fopen(argv[1],"r");

		/*Get the positions at which the works will be starting their parsing*/
		position = startPositions(file,workerCount);

		//for each worker
		/*for (i=0;i<workerCount;i++){
			if (i != workerCount-1){
				length = position[i+1]-position[i];
			}
			else{
				length = fileSize(file)-position[i];
			}

			partDataset(file,position[i],length);
		}*/

		Dataset *d = partDataset(file,SIZE_HEADER+SIZE_EOL,fileSize(file));
		for (i=0;i<workerCount;i++){
			printf("Worked %d index: %d\n",i,position[i]);
		}
		printf("\nFound %d collisions in %d records.\n",d->colNum,d->recNum);

        fclose(file);

        return 0;
}

