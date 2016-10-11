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
#define COL_VEHN 6				//Number of vehicles involved in collision
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
	int location;	            //Code of accident location 1-12 and QQ
	char gender;                //Gender of individual involved
	int vehYear;                //Vehicle year involved in collision
	int vehNum;					//Number of vehicles involved in collision
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
	Date fatal[14];
	Date nonFatal[14];
}WorstMonth;

typedef struct MostVehicles MostVehicles;
typedef struct MostVehicles {
	Date date;
	int total;
}MostVehicles;

typedef struct NewWreckedCars{
	int totalVehs;
	int totalCrashed;
	int vehAgeSum;
}NewWreckedCars;

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
static bool diffCol(char *rec1, char *rec2);

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
int count=0;
static bool diffCol(char *rec1, char *rec2){
	int i;
	char vtype[2][3],vyear[2][5],vid[2][3];
	bool caseOne,caseTwo,caseThree, yearOrTypeChange;	

	/*Store vehicle type*/
	vtype[0][0] = rec1[36];
	vtype[0][1] = rec1[37];
	vtype[0][2] = '\0';
	vtype[1][0] = rec2[36];
	vtype[1][1] = rec2[37];
	vtype[1][2] = '\0';

	/*Store vehicle id*/
	vid[0][0] = rec1[33];
	vid[0][1] = rec1[34];
	vid[0][2] = '\0';
	vid[1][0] = rec2[33];
	vid[1][1] = rec2[34];
	vid[1][2] = '\0';

	/*Store vehicle year*/
	for (i=0;i<4;i++){
		vyear[0][i] = rec1[39+i];
	}
	
	for (i=0;i<4;i++){
		vyear[1][i] = rec2[39+i];
	}
	vyear[0][4] = '\0';
	vyear[1][4] = '\0';
	
	caseOne = !strncmp(rec1,rec2,LENGTH_COLL);	//coll level information same
	caseTwo = (strcmp(vid[0],"01") != 0) && (strcmp(vid[1],"01") == 0);		//vehicle id changed to 1
	yearOrTypeChange = ( (strcmp(vyear[0],vyear[1]) != 0) || (strcmp(vtype[0],vtype[1]) != 0) );
	caseThree = (strcmp(vid[0],"01") == 0) && (strcmp(vid[1],"01") == 0) && yearOrTypeChange;	//Vehicle type or vehicle year changed

	/*if (caseOne && caseTwo && !caseThree){
		count++;
		printf("count:%d, vtype[0]:%s,vtype[1]:%s,vid[0]:%s,vid[1]:%s,vyear:%s,vyear:%s\n"
			,count,vtype[0],vtype[1],vid[0],vid[1],vyear[0],vyear[1]);
	}*/
	if ((caseOne==false)||(caseTwo==true)||(caseThree==true)){count++;}
	return ((caseOne==false) || (caseTwo==true) || (caseThree==true));

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
		while (!diffCol(prevLine,curLine)){
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
			
			case COL_VEHN:
				record->vehNum = strtol(token,NULL,10);

			case COL_LOC:
				record->location = strtol(token,NULL,10);

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
			
			if (diffCol(prevLine,line)){
				/*If not part of the same collision than create new index*/
				dataset->colNum++;
				addIndex(dataset, recCount(dataset));
			}
		}
		else{
			addIndex(dataset,1);
		}
	} 
	printf("Finished all diffCol's, count:%d\n",count); 
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

WorstMonth *findMax(int ***arr){
	int i,j,index=0,max=0;
	int maxF=0,indexF=0,maxFAll=0,maxAllF=0,maxAll=0;
	int indexYear=0,indexMonth=0;
	int months[12][2];
	WorstMonth *worst = malloc(sizeof(WorstMonth));

	for (i=0;i<12;i++){
		months[i][0] = 0;
		months[i][1] = 0;
	}
	
	for (j=0;j<14;j++){
		max = 0;
		maxF = 0;
		for (i=0;i<12;i++){

			/*Check for worst non fatal injury month*/
			if (arr[j][i][0] > max){
				worst->nonFatal[j].month = i+1;
				worst->nonFatal[j].year = j+1999;
				max = arr[j][i][0];
			}
			/*Check for worst fatal injury month*/
			if (arr[j][i][1] > maxF){
				worst->fatal[j].month = i+1;
				worst->fatal[j].year = j+1999;
				maxF = arr[j][i][1];
			}	
			/*Total all months*/
			months[i][0] += arr[j][i][0];
			months[i][1] += arr[j][i][1];
		}
	}

	/*Find worst month total*/
	max = maxF = 0;
	for (i=0;i<12;i++){
		if (months[i][0] > max){
			max = months[i][0];
			index = i;
		}
		if (months[i][1] > maxF){
			maxF = months[i][1];
			indexF = i;
		}
	}
	worst->total.month = index;
	worst->totalF.month = indexF;	

	return worst;
}

int *genderKilled(Dataset *dataset){
	int i, men=0, women=0,*genderKilled;
	
	genderKilled = malloc(sizeof(int)*2);

	for (i=0;i<dataset->recNum;i++){
		if (dataset->records[i]->gender == 'M'
			&& dataset->records[i]->death == true){
			
			men++;
		}
		if (dataset->records[i]->gender == 'F'
			&& dataset->records[i]->death == true){
			
			women++;
		}
	}
	genderKilled[0] = men;
	genderKilled[1] = women;

	return genderKilled;
}

MostVehicles *mostVehicles(Dataset *dataset){
	MostVehicles *mostVeh = malloc(sizeof(MostVehicles));
	int i,index;

	mostVeh->total = 0;	

	for (i=0;i<dataset->colNum;i++){
		index = dataset->collisionIndex[i];	
		
		if (dataset->records[index]->vehNum > mostVeh->total){
			mostVeh->total = dataset->records[index]->vehNum;
			mostVeh->date.year = dataset->records[index]->date.year;
			mostVeh->date.month = dataset->records[index]->date.month;
			mostVeh->date.day = dataset->records[index]->date.day;
		}	
	}
	return mostVeh;
	 
}

NewWreckedCars *countNewWrecks(Dataset *dataset){
	NewWreckedCars *newWrecks = malloc(sizeof(NewWreckedCars));
	int i;
	
	newWrecks->totalCrashed = 0;
	newWrecks->vehAgeSum = 0;
	newWrecks->totalVehs = 0;

	for (i=0;i<dataset->recNum;i++){
		
		if (dataset->records[i]->vehYear >= dataset->records[i]->date.year){	
			newWrecks->totalCrashed++;
		}
		/*Check to make sure year is rpesent*/
		if (dataset->records[i]->vehYear > 1000){	
			newWrecks->totalVehs++;
			newWrecks->vehAgeSum += dataset->records[i]->date.year - dataset->records[i]->vehYear + 1;
		}
	}
	return newWrecks;
}

int *countLocations(Dataset *dataset){
	int *locs = malloc(sizeof(int)*13);	
	int i;
	int index;

	for (i=0;i<13;i++){ locs[i] = 0; }
	
	for (i=0;i<dataset->colNum;i++){
		index = dataset->collisionIndex[i];
		
		locs[dataset->records[index]->location]++;
	}

	return locs;
}
#define W 1

PI_PROCESS *worker[W];
PI_CHANNEL *toWorker[W];
PI_CHANNEL *fromWorker[W];
PI_BUNDLE *toAllWorkers;
PI_BUNDLE *fromAllWorkers;

int workerJob(int num, void *fileName){
	MostVehicles *mostVeh;
	NewWreckedCars *wrecks;
	FILE *file;
	Dataset *dataset;
	int i,j,k,queryNum,curQuery;
	int numPos,*position,length;
	int ***colls;
	int *killed;
	int *locs;
	
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

	for (i=0;i<queryNum;i++){
		/*Perform certain calculation based on query*/
		switch(curQuery){
			case 1:
				/*Write amount of collisions found each month*/
				colls = collEachMonth(dataset);
				for (k=0;k<14;k++){
					for (j=0;j<12;j++){
						PI_Write(fromWorker[num], "%d %d %^d",k+1,j+1,2,colls[k][j]);
					}
				}	
				break;
			case 2:
				killed = genderKilled(dataset);
				PI_Write(fromWorker[num], "%d %d",killed[0],killed[1]);
				break;
			case 3:
				mostVeh = mostVehicles(dataset);
				PI_Write(fromWorker[num],"%d %d %d %d",mostVeh->total,mostVeh->date.year,mostVeh->date.month,mostVeh->date.day);
				break;
			case 4:
				wrecks = countNewWrecks(dataset);
				PI_Write(fromWorker[num],"%d %d %d",wrecks->totalCrashed,wrecks->vehAgeSum,wrecks->totalVehs);
				break;
			case 5:
				locs = countLocations(dataset);
				PI_Write(fromWorker[num],"%^d",13,locs);
				break;
		}
		/*Get the next query unless last iteration*/
		if (i!= queryNum-1){
			PI_Read(toWorker[num],"%d %d",&queryNum,&curQuery);
		}
	}	

	#ifdef DEBUG
	printf("Worker(%d) exiting.\n",num+1);
	#endif
	
	return 0; 
}

void processQueryOne(Dataset *dataset){
	int ***colAmount;
	int *colls;
	int j,k,done,month,year,size;
	WorstMonth *worst;

	/*Initialize array*/
	colAmount = malloc(sizeof(int**)*14);
	for (j=0;j<14;j++){
		colAmount[j] = malloc(sizeof(int*)*12);
		for (k=0;k<12;k++){
			colAmount[j][k] = malloc(sizeof(int)*2);
			colAmount[j][k][0] = 0;
			colAmount[j][k][1] = 0;
		}
	}

	/*Collect each workers findings for each month*/
	if (W>1){
		for (j=0;j<W*14*12;j++){
			done = PI_Select(fromAllWorkers);
			PI_Read(fromWorker[done],"%d %d %^d",&year,&month, &size, &colls);

			colAmount[year-1][month-1][0] += colls[0];
			colAmount[year-1][month-1][1] += colls[1];	

		}
	}
	else{
		colAmount = collEachMonth(dataset);
	}
		
	/*Print results*/
	worst = findMax(colAmount);
	for (j=0;j<14;j++){
		printf("$Q1,%d,%d,%d\n",worst->nonFatal[j].year,worst->nonFatal[j].month,worst->fatal[j].month);	
	}
	printf("$Q1,9999,%d,%d\n",worst->total.month,worst->totalF.month);
}
void processQueryTwo(Dataset *dataset){
	int done,i,men,women;
	int menTotal=0, womenTotal=0;
	int *gender;

	if (W > 1){	
		for (i=0;i<W;i++){
			done = PI_Select(fromAllWorkers);
			PI_Read(fromWorker[done],"%d %d",&men,&women);
			menTotal += men;
			womenTotal += women;	
		}
	}
	else{
		gender = genderKilled(dataset);	
		menTotal = gender[0];
		womenTotal = gender[1];	
	}
	printf("$Q2,%d,%d,%.2f,%.2f\n",menTotal,womenTotal
		,(double)menTotal/(menTotal+womenTotal),(double)womenTotal/(menTotal+womenTotal));
}

void processQueryThree(Dataset *dataset){
	MostVehicles **mostVeh;
	int i,max=0,index,done;

	mostVeh = malloc(sizeof(MostVehicles*)*W);
	for (i=0;i<W;i++){
		mostVeh[i] = malloc(sizeof(MostVehicles));
	}
	
	if (W > 1){
		for (i=0;i<W;i++){
			done = PI_Select(fromAllWorkers);
			PI_Read(fromWorker[done],"%d %d %d %d",&mostVeh[i]->total,&mostVeh[i]->date.year,&mostVeh[i]->date.month,&mostVeh[i]->date.day);
		}
		for (i=0;i<W;i++){
			if (mostVeh[i]->total > max){
				index = i;
				max = mostVeh[i]->total;
			}
		}
	}
	else{
		mostVeh[0] = mostVehicles(dataset);
		index = 0;
	}

	printf("$Q3,%d,%d,%d,%d\n",mostVeh[index]->total,mostVeh[index]->date.year,mostVeh[index]->date.month,mostVeh[index]->date.day);
}

void processQueryFour(Dataset *dataset){
	NewWreckedCars *wrecks = malloc(sizeof(NewWreckedCars));
	int i,done,crashes,veh,age;

	wrecks->totalCrashed = 0;
	wrecks->vehAgeSum = 0;
	wrecks->totalVehs = 0;

	if (W > 1){
		for (i=0;i<W;i++){
			done = PI_Select(fromAllWorkers);
			PI_Read(fromWorker[done],"%d %d %d",&crashes,&age,&veh);
			wrecks->totalCrashed += crashes;
			wrecks->vehAgeSum += age; 
			wrecks->totalVehs += veh;
		}
	}
	else{
		wrecks = countNewWrecks(dataset);
	}

	printf("$Q4,%d,%.1f\n",wrecks->totalCrashed/14,(double)wrecks->vehAgeSum/wrecks->totalVehs);
}

void processQueryFive(Dataset *dataset){
	int i,j,done,*locs,size;
	int *locsTotal;
	int max=0,index;

	locsTotal = malloc(sizeof(int)*13);
	for (i=0;i<13;i++){ locsTotal[i] = 0; }
	
	if (W > 1){
		for (i=0;i<W;i++){
			done = PI_Select(fromAllWorkers);
			PI_Read(fromWorker[done],"%^d",&size,&locs);
			
			for (j=0;j<13;j++){
				locsTotal[j] += locs[j];
			}
		}
	}
	else{
		locsTotal = countLocations(dataset);
	}

	/*Get most likely place*/
	for (i=0;i<13;i++){
		if (locsTotal[i] > max){
			max = locsTotal[i];
			index = i;
		}
	}
	printf("$Q5,%d",locsTotal[index]);
	for (i=1;i<13;i++){
		printf(",%d",locsTotal[i]);
	}
	printf(",%d\n",locsTotal[0]);
}

int main(int argc,char **argv){
	FILE *file;
	WorstMonth *worst;
	Dataset *dataset=NULL;
	int *colls,year,month,size;
	int *position,length[W];
	int N,i,j,k,done,queryNum;
	int recFound, recTotal,recReal,colFound,colTotal;
	int colAmount[14][12][2];	

	N = PI_Configure(&argc,&argv);	

	if (W > 1){
		
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

		recTotal = colTotal = 0;

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
	}
	else{	
		/*Open file and count records*/
		if ( (file = fopen(argv[1],"r")) == NULL){
			printf("Error: File not provided or could not be opened. Exiting.");
			return(EXIT_FAILURE);
		}
		recReal = (fileSize(file)-SIZE_HEADER-SIZE_EOL)/(SIZE_RECORD+SIZE_EOL);	
		dataset = partDataset(file,SIZE_HEADER+SIZE_EOL,fileSize(file)-SIZE_HEADER-SIZE_EOL);
		recTotal = dataset->recNum;
		colTotal = dataset->colNum;
	}

	for (i=1;i<argc-1;i++){

		/*Send all query requests to workers*/
		queryNum = strtol(argv[i+1],NULL,10);
		
		if (W > 1){
			PI_Broadcast(toAllWorkers,"%d %d",argc-2, queryNum);
		}

		switch(queryNum){
			case 1:
				processQueryOne(dataset);
				break;
			case 2:
				processQueryTwo(dataset);
				//Who is more likely to be killed in a collision? Men or women?
				break;
			case 3:
				processQueryThree(dataset);
				//Most number of vehicles crashed on which day?
				break;
			case 4:
				processQueryFour(dataset);
				//How many people wreck their new car, average vehicle age
				break;
			case 5:
				processQueryFive(dataset);
				//Where is the most likely place to have a collision?
					break;
		}	
	}
	

	#ifdef DEBUG	
	printf("PI_Main(Master): Calculated %d records present in file based on file size.\n",recReal);
	printf("PI_Main(Master): Received a total of %d records and %d collisions from %d workers.\n",recTotal,colTotal,W);
	#endif
	
	if (W > 1){
		PI_StopMain(0);
	}

	return 0;
}

