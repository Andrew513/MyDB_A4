
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include <iostream>

using namespace std;

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	int rootPageId = getTable()->lastPage() + 1;
	int leafPageId = rootPageId + 1;
	getTable()->setLastPage(leafPageId + 1);

	// create a new root page
	MyDB_PageReaderWriter rootPage = (*this)[rootPageId];
	rootPage.clear();
	rootPage.setType(DirectoryPage);

	// append a new internal record with key = infinity
	MyDB_INRecordPtr firstInRecord = make_shared<MyDB_INRecord>(getKey(getEmptyRecord()));
	rootPage.append(firstInRecord);

	// update the root location
	getTable()->setRootLocation(getTable()->lastPage());
	rootLocation = getTable()->getRootLocation();

	// set the pointer of the new in record to the new leaf page 
	firstInRecord->setPtr(getTable()->lastPage() + 1);

	// create a new empty leaf page
	MyDB_PageReaderWriter leafPage = (*this)[leafPageId];
	leafPage.clear();
	leafPage.setType(RegularPage);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return nullptr;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return nullptr;
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int curPageNode, vector <MyDB_PageReaderWriter> &, MyDB_AttValPtr, MyDB_AttValPtr) {
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
	append(rootLocation, appendMe);
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	return nullptr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	MyDB_PageReaderWriter curPageNode = (*this)[whichPage];
	MyDB_RecordPtr iterateIntoMe = getEmptyRecord();
	MyDB_RecordIteratorPtr pageRecIter = curPageNode.getIterator(iterateIntoMe);
	if (curPageNode.getType() == RegularPage) {
		// reach leaf page, append appendMe
		if (!(pageRecIter->hasNext() && curPageNode.append(appendMe))) {
			// leaf page is full, need to split
			MyDB_RecordPtr splitedInRecord = split(curPageNode, appendMe);
			return splitedInRecord;
		}
	} else {
		// iterate all inRecords in curPageNode
		while (pageRecIter->hasNext()) {
			// compare inRecord's attribute with appendMe's attribute, appendMe's smaller, recursively append appendMe

		}
	}

	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
