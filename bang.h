#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

#define SIZE_RECORD 61
#define SIZE_HEADER 145
#define SIZE_EOL 2
#define LENGTH_COLL 32			//Length of characters representing collision level data
#define COL_COUNT 22
#define COL_CYEAR 1
#define COL_MNTH 2
#define COL_DAY 3
#define COL_SEV 5
#define COL_LOC 8
#define COL_VYEAR 15
#define COL_SEX 17

typedef struct Date Date;
typedef struct Date {
	int year;			//Year collision occured
	int month;			//Month collision occured ranging 1-12, -1 for unspecified
	int day;			//Day collision occured ranging 1-7, -1 for unspecified
} Date;

typedef struct Record Record;
typedef struct Record {
	Date date;			//Date the collision occured
	char location[2];		//Code of accident location 1-12 and QQ
	char gender;		//Gender of individual involved
	int vehYear;		//Vehicle year involved in collision
	bool death;			//Whether the person involved in the collision died
} Record;

typedef struct Dataset Dataset;
typedef struct Dataset {
	int colNum;				//Number of collisions found in dataset
	int recNum;				//Number of records found in dataset
	int *collisionIndex;	//Array storing indexes of each collsion
	Record **records;		//Array of records addresses
} Dataset;

/*********************************************************************
 * FUNCTION NAME: getDataset
 * PURPOSE: Assigns specified number of workers to parse data from
 *			the file and return it.
 * ARGUMENTS: . File to be read from.
 *		      . Number of workers to do the job.
 * RETURNS: Address of Dataset containing all records found in the 
 *			file.
 *********************************************************************/
Dataset *getDataset(FILE *file, int workerCount);
