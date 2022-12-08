#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation, const int attrCnt, const attrInfo attrList[])
{
	// Check for NULL attributes
	for (int i = 0; i <= attrCnt - 1; ++i) {
		if (attrList[i].attrValue == NULL)
			return ATTRTYPEMISMATCH; // Could be ATTRNOTFOUND
	}

	Status status;
	int relAttrCnt;
	AttrDesc *attrs;

	// Get relation's attributes
	status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
	if (status != OK)
		return status;

	// Prepare record	
	int entrySz = 0;
	for (int i = 0; i <= attrCnt - 1; ++i) 
		entrySz += attrList[i].attrLen;

	char entryData[entrySz];
	Record entry;
	entry.data = (void *)entryData;
	entry.length = entrySz;

	// Copy attributes over
	int mrk;
	for (int i = 0; i <= relAttrCnt - 1; ++i) {
		mrk = 0;
		for (int j = 0; j <= attrCnt - 1; ++j) {
			if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0 && attrs[i].attrType == attrList[j].attrType && attrs[i].attrLen == attrList[j].attrLen) {
				memcpy(entryData + attrs[i].attrOffset, attrList[j].attrValue, attrs[i].attrLen);
				mrk = 1;
				break;
			}
		}
		if (!mrk)
			return ATTRNOTFOUND;
	}

	// Insert
	InsertFileScan rel(relation, status);
	if (status != OK)
		return status;

	RID entryRID;
	status = rel.insertRecord(entry, entryRID);
	if (status != OK)
		return status;

	return OK;
}

