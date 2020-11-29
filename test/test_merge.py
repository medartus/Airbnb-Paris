import pytest
import numpy as np
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
	if(len(toInsert)>0):
		toInsert = np.delete(np.array(toInsert),7,1)
	else : 
		toInsert = np.array(toInsert)
	toDelete = np.array(toDelete)
	if(len(mockedInsert)>0):
		mockedInsert = np.delete(np.array(mockedInsert),7,1)
	else :
		mockedInsert = np.array(mockedInsert)
	mockedDelete = np.array(mockedDelete)


	if verbose:
		print("Merge :")
		print(len(toInsert),toInsert)
		print("Mock : ")
		print(len(mockedInsert),mockedInsert)
		print("Merge :")
		print(len(toDelete),toDelete)
		print("Mock : ")
		print(len(mockedDelete),mockedDelete)
	assert (toInsert == mockedInsert).all(), "Wrong insert"
	assert (toDelete == mockedDelete).all(), "Wrong delete"

def testMock1():
	TestMerging(MockCalendar.Mock1())

def testMock2():
	TestMerging(MockCalendar.Mock2())

def testMock3():
	TestMerging(MockCalendar.Mock3())

def testMock4():
	TestMerging(MockCalendar.Mock4())

def testMock5():
	TestMerging(MockCalendar.Mock5())

def testMock6():
	TestMerging(MockCalendar.Mock6())

def testMock7():
	TestMerging(MockCalendar.Mock7())

def testMock8():
	TestMerging(MockCalendar.Mock8())

def testMock9():
	TestMerging(MockCalendar.Mock9())

def testMock10():
	TestMerging(MockCalendar.Mock10())

def testMock11():
	TestMerging(MockCalendar.Mock11())

def testMock12():
	TestMerging(MockCalendar.Mock12())

def testMock13():
	TestMerging(MockCalendar.Mock13())

def testMock14():
	TestMerging(MockCalendar.Mock14())


