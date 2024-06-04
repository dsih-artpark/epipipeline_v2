import pandas as pd
import datetime

def FixTwoDates(resultDate: datetime, sampleDate:datetime) -> pd.Series:
    """_summary_: Attempts to fix logical inconsistency in dates - Sample date > Result date

    Args:
        sampleDate (_type_): Sample Date in datetime format
        resultDate (_type_): Result Date in datetime format

    Returns:
        _type_: If logical errors can be fixed, returns updated date(s). Else, returns original dates.
    """
    
    isinstance(resultDate, datetime) and isinstance(sampleDate, datetime), "Format the dates before applying this function"
    delta=resultDate-sampleDate
    
    if (pd.Timedelta(60, "d") < delta ) | (delta < pd.Timedelta(0, "d")):
        
        if (resultDate.day==sampleDate.month) & (resultDate.day in range(1,13)):  # fix result date with swap
            newResultDate=datetime.datetime(day=resultDate.month, month=resultDate.day, year=resultDate.year) #swap month & day  
            try:
                assert pd.Timedelta(0, "d") <= newResultDate-sampleDate <= pd.Timedelta(60, "d")
                return pd.Series([sampleDate, newResultDate])
            except AssertionError:
                return pd.Series([sampleDate,resultDate])
                
        elif (sampleDate.day==resultDate.month) & (sampleDate.day in range(1,13)): # fix sample date with swap
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
        
        elif (resultDate.day-sampleDate.month==1) & (resultDate.day in range(1,13)): #  fix result date with swap b/w month & day of sample date 
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
        return pd.Series([sampleDate, resultDate])  # returns original dates if unfixed