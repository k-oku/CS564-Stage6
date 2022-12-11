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
	for (int i = 0; i <= relAttrCnt - 1; ++i) 
		entrySz += attrs[i].attrLen;

	char entryData[entrySz];
	Record entry;
	entry.data = (void *)entryData;
	entry.length = entrySz;

	// Copy attributes over
	int mrk;
	int tmpi = 0;
	float tmpf = 0;
	for (int i = 0; i <= relAttrCnt - 1; ++i) {
		mrk = 0;
		for (int j = 0; j <= attrCnt - 1; ++j) {
			if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0 && attrs[i].attrType == attrList[j].attrType) {
				mrk = 1;
				if (attrs[i].attrType == 0) {
					memcpy(entryData + attrs[i].attrOffset, (char *)attrList[j].attrValue, attrs[i].attrLen);
					break;
				}
				if (attrs[i].attrType == 1) {
					tmpi = atoi((char *)attrList[j].attrValue);
					memcpy(entryData + attrs[i].attrOffset, (char *)&tmpi, attrs[i].attrLen);
					break;
				}
				if (attrs[i].attrType == 2) {
					tmpf = atof((char *)attrList[j].attrValue);	
					memcpy(entryData + attrs[i].attrOffset, (char *)&tmpf, attrs[i].attrLen);
					break;
				} 				
				return ATTRTYPEMISMATCH;
			}
		}
		if (!mrk)
			return ATTRTYPEMISMATCH;
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

