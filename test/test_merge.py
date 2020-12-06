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
    if(len(mockedInsert)>0):
        mockedInsert = np.delete(np.array(mockedInsert),7,1)
    else :
        mockedInsert = np.array(mockedInsert)


    if verbose:
        print("Merge :")
        print(len(toInsert),toInsert)
        print("Mock : ")
        print(len(mockedInsert),mockedInsert)
        print("Merge :")
        print(len(toDelete),toDelete)
        print("Mock : ")
        print(len(mockedDelete),mockedDelete)
        print("Assert : ")
        print(toInsert == mockedInsert)
        print(toDelete == mockedDelete)
    
    if len(toInsert) == len(mockedInsert):
        assert (toInsert == mockedInsert).all(), "Wrong insert Content"
    else:
        assert toInsert == mockedInsert, "Wrong insert Length"
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

def testMock12():
    TestMerging(MockCalendar.Mock12())

def testMock13():
    TestMerging(MockCalendar.Mock13())

def testMock14():
    TestMerging(MockCalendar.Mock14())

def testMock15():
    TestMerging(MockCalendar.Mock15())

def testMock16():
    TestMerging(MockCalendar.Mock16())
    
def testMock17():
    TestMerging(MockCalendar.Mock17())
    
def testMock18():
    TestMerging(MockCalendar.Mock18())
    
def testMock19():
    TestMerging(MockCalendar.Mock19())
    
def testMock20():
    TestMerging(MockCalendar.Mock20())
    
def testMock21():
    TestMerging(MockCalendar.Mock21())
    
def testMock22():
    TestMerging(MockCalendar.Mock22())
    
def testMock23():
    TestMerging(MockCalendar.Mock23())
    
def testMock24():
    TestMerging(MockCalendar.Mock24())
    
def testMock25():
    TestMerging(MockCalendar.Mock25())
    
def testMock26():
    TestMerging(MockCalendar.Mock26())

def testMock27():
    TestMerging(MockCalendar.Mock27())

def testMock28():
    TestMerging(MockCalendar.Mock28())

def testMock29():
    TestMerging(MockCalendar.Mock29())

def testMock31():
    TestMerging(MockCalendar.Mock31())


if __name__ == "__main__":
    TestMerging(MockCalendar.Mock20(),True)