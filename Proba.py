import random
import numpy as np
from datetime import datetime

def SizeWindow(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


def AddingProba(calendar):
    for row in calendar:
        if row['available'] == 't' or SizeWindow(row['start'], row['end']) < row['minimum_nights'] or SizeWindow(row['start'], row['end']) > row['maximum_nights']:
            row['Proba'] = 0
        elif SizeWindow(row['start'], row['end']) > 200:
            row['Proba'] = 0.01
        else:
            row['proba'] = 935/(142*np.sqrt(2*np.pi))*np.exp(-0.5*np.power((SizeWindow(row['start'], row['end'])+202)/142,2))
    calendar['Proba'] = [ random.randint(0,100)/100  for k in calendar.index]
    return calendar

AddingProba(calendar)
