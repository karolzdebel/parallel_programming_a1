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
#define LENGTH_COLL 32			//Characters representing collision level data
#define COL_COUNT 22            //Amount of columns found in each record
#define COL_CYEAR 1             //Collision year
#define COL_MNTH 2              //Collision month
#define COL_DAY 3               //Collision day
#define COL_SEV 5               //Collision severity
#define COL_LOC 8               //Collision location
#define COL_VYEAR 15            //Vehicle year involved in collision
#define COL_SEX 17              //Gender of individual involved in collision
#define FATA 0
#define COLL 1

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


typedef struct WorstMonth WorstMonth;
typedef struct WorstMonth {
	Date total;
	Date totalF;
	int fatal[14];
	int nonFatal[14];
}WorstMonth;

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
	dataset->recNum++;
	
	/*Allocate more memory for new record*/
	if (dataset->records != NULL){
		dataset->records = realloc(dataset->records,sizeof(Record*)*recCount(dataset));
		dataset->records[recCount(dataset)-1] = record;
	}
	else{
		dataset->records = malloc(sizeof(Record*));
		dataset->records[0] = record;
	}
}

static void addIndex(Dataset *dataset, int index){

	if (dataset->collisionIndex != NULL){
		dataset->collisionIndex = realloc(dataset->collisionIndex,sizeof(int)*colCount(dataset));
		dataset->collisionIndex[colCount(dataset)-1] = index-1;
	}
	else{
		dataset->collisionIndex = malloc(sizeof(int));
		dataset->collisionIndex[0] = index-1;
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
int ***collEachMonth(Dataset *dataset){
	int ***tally;
	int year,month;
	int i,j,index;

	tally = malloc(sizeof(int**)*14);
	for (i=0;i<14;i++){

		tally[i] = malloc(sizeof(int*)*12);
		for (j=0;j<12;j++){
			tally[i][j] = malloc(sizeof(int)*2);
			tally[i][j][0] = 0;
			tally[i][j][1] = 0;	
		}
	}
	//Count number of fatalities in each month and number of collisions in each month
	
	//Go through each collision record index
		//Add one to  tally[YEAR][MONTH][COLL]
		//Add one to tally[YEAR][MONTH][FATA] if fatality occurred
	for (i=0;i<dataset->colNum;i++){
		index = dataset->collisionIndex[i];
		
		month = dataset->records[index]->date.month;
		year = dataset->records[index]->date.year;

		if (month > 0 && month < 13){	
			tally[year-1999][month-1][0]++;
			if (dataset->records[index]->death){
				tally[year-1999][month-1][1]++;
			}
		}	
	}
	return tally;
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
		else{
			addIndex(dataset,1);
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

WorstMonth *findMax(int arr[14][12][2]){
	int i,j,index=0,max=0;
	int maxF=0,indexF=0,maxFAll=0,maxAllF=0,maxAll=0;
	int indexYear=0,indexMonth=0;
	WorstMonth *worst = malloc(sizeof(WorstMonth));
	
	for (j=0;j<14;j++){
		max = 0;
		maxF = 0;
		for (i=0;i<12;i++){
			/*Check for worst non fatal injury month*/
			if (arr[j][i][0] > max){
				worst->nonFatal[j] = i+1;
				max = arr[j][i][0];
			}
			/*Check for worst fatal injury month*/
			if (arr[j][i][1] > maxF){
				worst->fatal[j] = i+1;
				maxF = arr[j][i][1];
			}
			if (arr[j][i][0] > maxAll){
				maxAll = arr[j][i][0];
				worst->total.year = j+1999;
				worst->total.month = i+1;
			}
			if (arr[j][i][1] > maxAllF){
				maxAllF = arr[j][i][1];
				worst->totalF.year = j+1999;
				worst->totalF.month = i+1;
			}
		}
	}

	return worst;
}

#define W 6

PI_PROCESS *worker[W];
PI_CHANNEL *toWorker[W];
PI_CHANNEL *fromWorker[W];
PI_BUNDLE *toAllWorkers;
PI_BUNDLE *fromAllWorkers;


int workerJob(int num, void *fileName){
	FILE *file;
	Dataset *dataset;
	int i,j,queryNum,curQuery;
	int numPos,*position,length;
	int ***colls;
	
	#ifdef DEBUG
	printf("Worker(%d): Created and received filename: %s as 2nd argument\n"
		,num+1,(char*)fileName);
	#endif

	PI_Read(toWorker[num],"%^d",&numPos, &position);
	
	#ifdef DEBUG
	printf("Worker(%d): Called PI_Read on toWorker[%d](Channel) and received file index from master.\n",num+1,num);
	#endif

	file = fopen( (char*)fileName, "r");

	length = readLength(num,W,position,fileSize(file));		
	/*Get data in workers partition*/
	if ( (dataset = partDataset(file, position[num], length)) == NULL){
		printf("Error: Worker %d could not parse dataset.\n",num);
	}
	fclose(file);

	#ifdef DEBUG
	printf("Worker(%d): Finished reading file, data needs to be sent to master(PI_Main). Calling PI_Write on fromWorker[%d](Channel) with %d records and %d collisions\n"
		,num+1,num,dataset->recNum,dataset->colNum);
	#endif

	/*Write amount of records to master*/
	PI_Write(fromWorker[num], "%d %d", dataset->recNum, dataset->colNum);	

	/*Get the first query and the number of queries*/
	PI_Read(toWorker[num],"%d %d",&queryNum,&curQuery);
	printf("first query: %d num queries: %d\n",curQuery,queryNum);
	for (i=0;i<queryNum;i++){
		/*Perform certain calculation based on query*/
		switch(curQuery){
			case 1:
				/*Write amount of collisions found each month*/
				colls = collEachMonth(dataset);
				for (i=0;i<14;i++){
					for (j=0;j<12;j++){
						PI_Write(fromWorker[num], "%d %d %^d",i+1,j+1,2,colls[i][j]);
					}
				}	
				break;
			case 2:

				break;
			case 3:

				break;
			case 4:

				break;
			case 5:

				break;
		}
		/*Get the next query unless last iteration*/
		/*if (i!= queryNum-1){
			PI_Read(toWorker[num],"%d %d",&queryNum,&curQuery);

		}*/
	}	

	#ifdef DEBUG
	printf("Worker(%d) exiting.\n",num+1);
	#endif
	
	return 0; 
}

int main(int argc,char **argv){
	FILE *file;
	WorstMonth *worst;
	int *colls,year,month,size;
	int *position,length[W];
	int N,i,j,k,done,queryNum;
	int recFound, recTotal,recReal,colFound,colTotal;
	int colAmount[14][12][2];

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
	printf("PI_Main(Master): Broadcasting(PI_Broadcast) array of file indexes to toAllWorkers(BUNDLE).\n");
	#endif 

	PI_Broadcast(toAllWorkers,"%^d",W,position);

	recTotal = colFound = 0;
	/*Get number of records found by all workers
	and compare to number of records found by main*/
	for (i=0;i<W;i++){

		#ifdef DEBUGs
		printf("PI_Main(Master): Calling PI_Select on fromAllWorkers(CHANNEL), waiting a for worker to write.\n");
		#endif
		
		done = PI_Select(fromAllWorkers);
		
		#ifdef DEBUG
		printf("PI_Main(Master): PI_Select on fromAllWorkers(CHANNEL) returned %d, calling PI_Read on fromWorker[%d] to get record and collision numbers found by worker %d. \n"
			,done,done,done +1);
		#endif

		PI_Read(fromWorker[done],"%d %d", &recFound,&colFound);

		#ifdef DEBUG
		printf("PI_Main(Master): Received a count of %d records and %d collision from worker %d through fromWorker[%d](CHANNEL)\n",recFound,colFound,done+1,done);
		#endif

		colTotal += colFound;
		recTotal += recFound;
	}
	for (i=1;i<argc-1;i++){

		/*Send all query requests to workers*/
		queryNum = strtol(argv[i+1],NULL,10);
		printf("Query num in main %d\n",queryNum);
		PI_Broadcast(toAllWorkers,"%d %d",argc-2, queryNum);

		switch(queryNum){
			case 1:
				/*Initialize array*/
				for (j=0;j<14;j++){
					for (k=0;k<12;k++){
						colAmount[j][k][0] = 0;
						colAmount[j][k][1] = 0;
					}
				}

				/*Collect each workers findings for each month*/
				for (j=0;j<W*14*12;j++){
					done = PI_Select(fromAllWorkers);
					PI_Read(fromWorker[done],"%d %d %^d",&year,&month, &size, &colls);
		
					colAmount[year-1][month-1][0] += colls[0];
					colAmount[year-1][month-1][1] += colls[1];	
					//(Worst Month)Fill array[14][12][2]
				}
				/*Print results*/
				worst = findMax(colAmount);
				for (j=0;j<14;j++){
					printf("Highest non-fatal %d/%d, fatal: %d/%d\n",1999+j,worst->nonFatal[j],j+1999,worst->fatal[j]);	
				}
				printf("Overall worst non fatal: %d/%d, fatal: %d/%d\n",worst->total.year,worst->total.month,worst->totalF.year,worst->totalF.month);
					
				break;
			case'2':
				//Who is more likely to be killed in a collision? Men or women?
				break;
			case '3':
				//Most number of vehicles crashed on which day?
				break;
			case '4':
				//How many people wreck their new car, average vehicle age
				break;
			case '5':
				//Where is the most likely place to have a collision?
					break;
		}	
	}
	

	#ifdef DEBUG	
	printf("PI_Main(Master): Calculated %d records present in file based on file size.\n",recReal);
	printf("PI_Main(Master): Received a total of %d records and %d collisions from %d workers.\n",recTotal,colTotal,W);
	#endif

	PI_StopMain(0);

	return 0;
}

