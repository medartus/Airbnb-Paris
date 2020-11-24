import pytest

import MockCalendar

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

import MergeCalendar


def TestMerging(fct):
	actual_calendar, new_calendar, mockedInsert, mockedDelete = fct
	toInsert, toDelete = MergeCalendar.MergeTwoCalendars(actual_calendar,new_calendar)

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