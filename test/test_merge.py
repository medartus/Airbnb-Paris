import pytest

import MockCalendar

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

import MergeCalendar


def TestMerging(fct, verbose=False):
	actual_calendar, new_calendar, mockedInsert, mockedDelete = fct
	toInsert, toDelete = MergeCalendar.MergeTwoCalendars(actual_calendar,new_calendar)

	if verbose:
		print(len(toInsert),toInsert)
		print()
		print(len(mockedInsert),mockedInsert)
		print()
		print(len(toDelete),toDelete)
		print()
		print(len(mockedDelete),mockedDelete)

	assert toInsert == mockedInsert, "Wrong insert"
	assert toDelete == mockedDelete, "Wrong delete"

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

def testMock13():
	TestMerging(MockCalendar.Mock13())

def testMock14():
	TestMerging(MockCalendar.Mock14())