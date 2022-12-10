#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
	//make sure to handle cases such as when input argument is NULL
	//scan depends on attr type: INTEGER, etc.

	// This function will delete all tuples in relation satisfying the predicate 
	// by attrName, op, and the constant attrValue. 
	//type denotes the type of the attribute. 
	//You can locate all the qualifying tuples using a filtered HeapFileScan.
	//per
	//handle case wher input is NULL

	Status status;
	HeapFileScan relScan(relation, status);
	RID rRID;
	int tupCnt = 0; //keep track of how many tuples will be deleted

	if (status != OK) {
		//relation is NULL
		return status;
	
	}
	//if attrName is NULL
	if(attrName.length() == 0) {
		//delete all rows in relation
		status = relScan.startScan(0,0, STRING, NULL, EQ);
		if (status != OK) {
			return status;
		}

		while(relScan.scanNext(rRID) == OK) {
			status = relScan.deleteRecord();
			if (status != OK) {
				return status;
			}
			tupCnt++;
			printf("deleted %d tuples \n", tupCnt);
			return OK;
		}
	}

	AttrDesc attrDesc;
	//otherwise attr catalog tuple for search
	status = attrCat->getInfo(relation, attrName, attrDesc);

	if (status != OK) {
		return status;
	}
	
	int tmpI;
	int tmpF;
	const char* filter;
	//scan depends on data type, convert to proper data type
	switch(type) {
		case INTEGER:
			tmpI = atoi(attrValue);
			filter = (char*) &tmpI;
			break;
		case FLOAT: 
			tmpF = atof(attrValue);
			filter = (char*)&tmpF;
			break;
		case STRING:
			filter = attrValue;
			break;
	}

	//now can scan through tuples
	status = relScan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, filter, op);
	if (status != OK) {
		return status;
	}

	while(relScan.scanNext(rRID) == OK) {
		//match found, delete tuple
		status = relScan.deleteRecord();
		if(status != OK) {
			return status;
		}
		tupCnt++;
	}

	printf("deleted %d tuplesz \n", tupCnt);
	
	//if tuples deleted w no issues
	return OK;



}
