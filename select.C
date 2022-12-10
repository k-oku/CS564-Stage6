#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
   // join.C provides what we should provide for ScanSelect in QU_NL_Join
    cout << "Doing QU_Select " << endl;
	Status status; 
	// by setting things up, it means to give scanselect proper info. 
	// need to consult catalog. that is to say, the global vars attrCat and relCat,
	// to go from attrInfo to attrDesc, which scanselect needs.
	AttrDesc attrDesc;
	int reclen; // length of output tuple
	// there are multiple descriptions needed for projections
	AttrDesc* projDesc = new AttrDesc[projCnt];
	// populate this array with proper descs using getInfo from AttrCatalog
	// getInfo(relName, attrName, array)
	for (int i = 0; i < projCnt; i++) {
		// need to increment length of output tuple here
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
		reclen += projDesc[i].attrLen; // add attrLen to reclen to get whole tuple length
		if (status != OK) {
			return status;
		}
	}
	// need to pass in proper filter
	// depends on data type in attr
	// You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.
	// If attr is NULL, an unconditional scan of the input table should be performed.
	// the search value is always supplied as the character string attrValue.
	// You should convert it to the proper type based on the type of attr.
	// The project list is defined by the parameters projCnt and projNames.
	// Projection should be done on the fly as each result tuple is being appended to the result table.
	const char* filter;
	if (attr == NULL) {
		// unconditional scan
		// copy over relnames and attrnames
		strcpy(attrDesc.relName, projNames[0].relName);
		strcpy(attrDesc.attrName, projNames[0].attrName);
		// set every int to 0 (default)
		attrDesc.attrOffset = 0;
		attrDesc.attrType = INTEGER; // has to be set to something
		attrDesc.attrLen = 0;
		// filter is null, use EQ for op
		status = ScanSelect(result, projCnt, projDesc, &attrDesc, EQ, NULL, reclen);
		if (status != OK) {
			return status;
		}
		return OK;
	} else if (attr->attrType == INTEGER) {
		status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
		if (status != OK) {return status;}
		int tmp = atoi(attrValue);
		filter = (char*)&tmp;
	} else if (attr->attrType == STRING) {
		status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
		if (status != OK) {return status;}
		filter = attrValue; // string already
	} else if (attr->attrType == FLOAT) {
		status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
		if (status != OK) {return status;}
		float tmp = atof(attrValue);
		filter = (char*)&tmp;
	}
	status = ScanSelect(result, projCnt, projDesc, &attrDesc, op, filter, reclen);
	if (status != OK) {
		return status;
	}
	return OK;

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	// A selection is implemented using a filtered HeapFileScan. 
	// join.C provides good implementation notes of this <<
	// The result of the selection is stored in the result relation called result (passed in)
	// (a  heapfile with this name will be created by the parser before QU_Select() is called)

	Status status;
	int resultTupCnt = 0;
	// following join.C, QU_NL_Join
	// open the result table
	InsertFileScan resultRel(result, status);
	if (status != OK) {
		return status;
	}
	char outputData[reclen];
    Record outputRec; // for output
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
	// set up, scan outer
	HeapFileScan outerScan(string(attrDesc->relName), status);
	if (status != OK) {
		return status;
	}
	status = outerScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
	if (status != OK) {
		return status;
	}
	RID outerRID;
	Record outerRec;
	// scanning outer table
	while(outerScan.scanNext(outerRID) == OK) {
		status = outerScan.getRecord(outerRec);
        ASSERT(status == OK);
		// if here, match found
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++) {
			// only one attr, unlike join.C
			memcpy(outputData + outputOffset, (char *)outerRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
		}
		// add the new record to the output relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        ASSERT(status == OK);
        resultTupCnt++;
	}
	// all done
	printf("ScanSelect selected %d result tuples \n", resultTupCnt); // this is in join.C, need it for something?
	return OK;

}
