
import random

def AddingProba(calendar):
    calendar['Proba'] = [ random.randint(0,100)/100  for k in calendar.index]
    return calendar