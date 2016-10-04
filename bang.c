#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include "pilot.h"

#define DEBUG
#define SIZE_RECORD 61          //Length of a record
#define SIZE_HEADER 145         //Lenght of a header
#define SIZE_EOL 2              //Amount of EOL characters in each line
#define LENGTH_COLL 32          //Length of characters representing collision level data
#define COL_COUNT 22            //Amount of columns found in each record
#define COL_CYEAR 1             //Collision year
#define COL_MNTH 2              //Collision month
#define COL_DAY 3               //Collision day
#define COL_SEV 5               //Collision severity
#define COL_LOC 8               //Collision location
#define COL_VYEAR 15            //Vehicle year involved in collision
#define COL_SEX 17              //Gender of individual involved in collision

typedef struct Date Date;
typedef struct Date {
	int year;                   //Year collision occured
	int month;                  //Month collision occured ranging 1-12, -1 for unspecified
	int day;                    //Day collision occured ranging 1-7, -1 for unspecified
} Date;

typedef struct Record Record;
typedef struct Record {
	Date date;                  //Date the collision occured
	char location[2];           //Code of accident location 1-12 and QQ
	char gender;                //Gender of individual involved
	int vehYear;                //Vehicle year involved in collision
	bool death;                 //Whether the person involved in the collision died
} Record;

typedef struct Dataset Dataset;
typedef struct Dataset {
	int colNum;                 //Number of collisions found in dataset
	int recNum;                 //Number of records found in dataset
	int *collisionIndex;        //Array storing indexes of each collsion
	Record **records;           //Array of records addresses
} Dataset;


/******************HELPER FUNCTION DOCUMENTATION*********************/
/*********************************************************************
 * FUNCTION NAME: partDataset
 * PURPOSE: Stores record data and collision indexes from a 
 *          specified partition of the file.
 * ARGUMENTS: . File to be read from.
 *            . Position of the file to start reading from.
 *            . How many records to read.
 * RETURNS: Address of Dataset containing all records found in the 
 *          portion provided.
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
 *            . Record being compared.
 * RETURNS: True if the records are part of the same collision,
 *          false otherwise.
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
 *          start at.
 * ARGUMENTS: . File being read by workers.
 *            . Number of workers performing the job.
 * RETURNS: Integer array of size workerCount containg the indexes 
 *          for each worker.
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
 *          data structure.
 * ARGUMENTS: . Dataset data structure address.
 * RETURNS: Integer containg the number of records found.
 *********************************************************************/
static int recCount(Dataset *dataset);

/*********************************************************************
 * FUNCTION NAME: colCount
 * PURPOSE: Gets the number of collision found in the dataset
 *          data structure.
 * ARGUMENTS: . Dataset data structure address.
 * RETURNS: Integer containg the number of collisions.
 *********************************************************************/
static int colCount(Dataset *dataset);

/*********************************************************************
 * FUNCTION NAME: addRecord
 * PURPOSE: Add record to the dataset data structure.
 * ARGUMENTS: . Dataset to add to.
 *            . Record being added.
 *********************************************************************/
static void addRecord(Dataset *dataset, Record *record);

/*********************************************************************
 * FUNCTION NAME: addIndex
 * PURPOSE: Adds index of a collision to the dataset data structure.
 * ARGUMENTS: . Dataset collision took part in.
 *            . Index of collision.
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
	int *index = malloc(sizeof(int)*workerCount);   //File position for each worker to start at
	int totalSize = fileSize(file);
	int sizeLeft = fileSize(file);
	int approxIndex = 0;
	int offset = 0, curWorker = 0;
	char curLine[SIZE_RECORD+1],prevLine[SIZE_RECORD+1];

	/*Account for header*/
	totalSize -= SIZE_HEADER+SIZE_EOL;
	sizeLeft -= SIZE_HEADER+SIZE_EOL;

	/*Worker 1 always starts at beginning*/
	index[0] = SIZE_HEADER+SIZE_EOL;

	/*Calculate where in the file each worker should start their job*/
	for (curWorker=0;curWorker<workerCount-1;curWorker++){		

		approxIndex += sizeLeft/(workerCount-curWorker);  //Set to approximate index
		approxIndex += (SIZE_RECORD+SIZE_EOL)-(approxIndex%(SIZE_RECORD+SIZE_EOL));  //Align index with a record in file

		/*Seek to approximate index*/
		fseek(file,approxIndex+SIZE_HEADER+SIZE_EOL,SEEK_SET);

		/*Read first two lines*/
		fread((void*)prevLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
		prevLine[SIZE_RECORD] = '\0';
		fread((void*)curLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
		curLine[SIZE_RECORD] = '\0';

		offset = SIZE_RECORD+SIZE_EOL;
		/*Calculate how many records ahead of approximate index 
		are part of the same collision*/
		while (sameCol(prevLine,curLine)){
			offset += SIZE_RECORD+SIZE_EOL;
			strcpy(prevLine,curLine);
			fread((void*)curLine,sizeof(char),SIZE_RECORD+SIZE_EOL,file);
			curLine[SIZE_RECORD] = '\0';
		}
		index[curWorker+1] = approxIndex+offset+SIZE_HEADER+SIZE_EOL;       //Set workers index to last record of a collision
		sizeLeft = totalSize-index[curWorker+1];              //Update the amount of work left
	}

	return index;
}

static Record *getRecord(char *recLine){
	int col;        //column
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
	if (fseek(file,startPos,SEEK_SET) == -1){
		return NULL;
	}

	/*Calculate number of reads based on length provided*/
	readCount = (readLength)/(SIZE_RECORD+SIZE_EOL);

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

static int readLength(int workerNum, int workerCount, int position[], int fileSize){
	/*Get length of partition*/
	if (workerNum != (workerCount-1)){
    	return position[workerNum+1]-position[workerNum]; 
	}		
	else{
		return fileSize-position[workerNum];
	}		
}

#define W 7

PI_PROCESS *worker[W];
PI_CHANNEL *toWorker[W];
PI_CHANNEL *fromWorker[W];
PI_BUNDLE *toAllWorkers;
PI_BUNDLE *fromAllWorkers;


int workerJob(int num, void *fileName){
	FILE *file;
	Dataset *dataset;
	int numPos,*position,length,i;
	
	#ifdef DEBUG
	printf("Worker(%d) has been created and received filename: %s as 2nd argument\n"
		,num+1,(char*)fileName);
	#endif

	PI_Read(toWorker[num],"%^d",&numPos, &position);
	
	#ifdef DEBUG
	printf("Worker(%d) PI_Read array storing position to begin reading.\n",num+1);
	#endif

	file = fopen( (char*)fileName, "r");

	length = readLength(num,W,position,fileSize(file));		
	/*Get data in workers partition*/
	if ( (dataset = partDataset(file, position[num], length)) == NULL){
		printf("Error: Worker %d could not parse dataset.\n",num);
	}
	fclose(file);

	#ifdef DEBUG
	printf("Worker(%d) finished reading file. PI_Write is being called on fromWorker[%d] channel with %d records and %d collisions.\n"
		,num+1,num,dataset->recNum,dataset->colNum);
	#endif

	/*Write amount of records to master*/
	PI_Write(fromWorker[num], "%d %d", dataset->recNum, dataset->colNum);	

	#ifdef DEBUG
	printf("Worker(%d) exiting.\n",num+1);
	#endif
	
	return 0; 
}

int main(int argc,char **argv){
	FILE *file;
	int *position,length[W];
	int N,i,done;
	int recFound, recTotal,recReal,colFound,colTotal;

	/*Configure pilot*/	
	N = PI_Configure(&argc,&argv);

	/*Open file and count records*/
	if ( (file = fopen(argv[1],"r")) == NULL){
		printf("Error: File not provided or could not be opened. Exiting.");
		return(EXIT_FAILURE);
	}		
	recReal = (fileSize(file)-SIZE_HEADER-SIZE_EOL)/(SIZE_RECORD+SIZE_EOL);
	
	/*Create each worker and channels*/
	for (i=0;i<W;i++){

		worker[i] = PI_CreateProcess(workerJob, i, (void*)argv[1]);
		toWorker[i] = PI_CreateChannel(PI_MAIN, worker[i]);
		fromWorker[i] = PI_CreateChannel(worker[i],PI_MAIN);
	}

	fromAllWorkers = PI_CreateBundle(PI_SELECT,fromWorker,W);
	toAllWorkers = PI_CreateBundle(PI_BROADCAST, toWorker,W);

	PI_StartAll();

	position = startPositions(file,W);
	fclose(file);

	#ifdef DEBUG
	printf("PI_MAIN PI_Broadcast array of workers file indexes to toAllWorkers bundle.\n");
	#endif 

	PI_Broadcast(toAllWorkers,"%^d",W,position);

	recTotal = colFound = 0;
	/*Get number of records found by all workers
	and compare to number of records found by main*/
	for (i=0;i<W;i++){

		#ifdef DEBUG
		printf("PI_Main calling PI_Select on fromAllWorkers expecting record and collision numbers.\n");
		#endif

		done = PI_Select(fromAllWorkers);
		PI_Read(fromWorker[done],"%d %d", &recFound,&colFound);

		#ifdef DEBUG
		printf("PI_Main received a count of %d records and %d collision from worker %d\n",recFound,colFound,done+1);
		#endif

		colTotal += colFound;
		recTotal += recFound;
	}

	#ifdef DEBUG	
	printf("PI_Main calculated %d records present in file based on file size.\n",recReal);
	printf("PI_Main received a total of %d records and %d collisions from %d workers.\n",recTotal,colTotal,W);
	#endif

	PI_StopMain(0);

	return 0;
}

