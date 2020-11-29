import pytest

import MockCalendar

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

import MergeCalendar


def TestMerging(fct, verbose=False):
	new_calendar, actual_calendar, mockedInsert, mockedDelete = fct
	toInsert, toDelete = MergeCalendar.MergeTwoCalendars(actual_calendar,new_calendar)
	toInsert.sort(key = lambda x: (x[2],x[3]), reverse = False)
	toDelete.sort()
	mockedInsert.sort(key = lambda x:(x[2],x[3]),reverse = False)

	if verbose:
		print("Merge :")
		print(len(toInsert),toInsert)
		print("Mock : ")
		print(len(mockedInsert),mockedInsert)
		print("Merge :")
		print(len(toDelete),toDelete)
		print("Mock : ")
		print(len(mockedDelete),mockedDelete)
	assert toInsert == mockedInsert, "Wrong insert"
	assert toDelete == mockedDelete, "Wrong delete"

def testMock1():
	TestMerging(MockCalendar.Mock1(),verbose = True)

def testMock2():
	TestMerging(MockCalendar.Mock2(),verbose = True)

def testMock3():
	TestMerging(MockCalendar.Mock3(),verbose = True)

def testMock4():
	TestMerging(MockCalendar.Mock4(),verbose = True)

def testMock5():
	TestMerging(MockCalendar.Mock5(),verbose = True)

def testMock6():
	TestMerging(MockCalendar.Mock6(), verbose = True)

def testMock7():
	TestMerging(MockCalendar.Mock7(), verbose = True)

def testMock8():
	TestMerging(MockCalendar.Mock8(), verbose = True )

def testMock9():
	TestMerging(MockCalendar.Mock9(), verbose = True)

def testMock10():
	TestMerging(MockCalendar.Mock10())

def testMock11():
	TestMerging(MockCalendar.Mock11())

def testMock13():
	TestMerging(MockCalendar.Mock13())

def testMock14():
	TestMerging(MockCalendar.Mock14())

testMock9()

