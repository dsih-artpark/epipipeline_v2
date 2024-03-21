import pandas as pd
import datetime

def date_fix(sampleDate, resultDate):
    """_summary_: Fixes logical inconsistency in dates - Sample date > Result date

    Args:
        sampleDate (_type_): Sample Date in datetime format
        resultDate (_type_): Result Date in datetime format

    Returns:
        _type_: ription__desc
    """
    delta=resultDate-sampleDate
    
    if (pd.Timedelta(60, "d") < delta ) | (delta < pd.Timedelta(0, "d")):
        
        if (resultDate.day==sampleDate.month) & (resultDate.day in range(1,13)):  # fix result date
            newResultDate=datetime.datetime(day=resultDate.month, month=resultDate.day, year=resultDate.year) #swap month & day  
            try:
                assert pd.Timedelta(0, "d") <= newResultDate-sampleDate <= pd.Timedelta(60, "d")
                return pd.Series([sampleDate, newResultDate])
            except AssertionError:
                return pd.Series([sampleDate,resultDate])
                
        elif (sampleDate.day==resultDate.month) & (sampleDate.day in range(1,13)): # fix sample date
            newSampleDate=datetime.datetime(day=sampleDate.month, month=sampleDate.day, year=sampleDate.year) #swap month & day
            try:
                assert pd.Timedelta(0, "d") <= resultDate-newSampleDate <= pd.Timedelta(60, "d")
                return pd.Series([newSampleDate, resultDate])
            except AssertionError:
                return pd.Series([sampleDate,resultDate])
            
        elif (sampleDate.day==resultDate.day) & (sampleDate.day in range(1,13)):  #fix both dates
            newSampleDate=datetime.datetime(day=sampleDate.month, month=sampleDate.day, year=sampleDate.year) # swap month & day
            newResultDate=datetime.datetime(day=resultDate.month, month=resultDate.day, year=resultDate.year) # swap month & day
            try:
                assert pd.Timedelta(0, "d") <= newResultDate-newSampleDate <= pd.Timedelta(60, "d")
                return pd.Series([newSampleDate,newResultDate])
            except AssertionError:
                return pd.Series([sampleDate, resultDate])
        
        elif (resultDate.day-sampleDate.month==1) & (resultDate.day in range(1,13)): # standalone fix to result date
            newResultDate=datetime.datetime(day=resultDate.month, month=resultDate.day, year=resultDate.year) # swap month & day
            try:
                assert pd.Timedelta(0, "d") <= newResultDate-sampleDate <= pd.Timedelta(60, "d")
                return pd.Series([sampleDate, newResultDate])
            except AssertionError:
                return pd.Series([sampleDate,resultDate])
            
        elif (sampleDate.day-resultDate.month==-1): #standalone fix to sample date
            newSampleDate=datetime.datetime(day=sampleDate.month, month=sampleDate.day, year=sampleDate.year)# swap month & day
            try:
                assert pd.Timedelta(0, "d") <= resultDate-newSampleDate <= pd.Timedelta(60, "d")
                return pd.Series([newSampleDate, resultDate])
            except AssertionError:
                return pd.Series([sampleDate,resultDate])
    else:
        return pd.Series([sampleDate, resultDate])