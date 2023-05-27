import pandas as pd
import numpy as np





def clean_data(df):
    # Drop the 'id', 'DocIDHash', 'DaysSinceCreation' columns
    df = df.drop(['ï»¿ID', 'DocIDHash', 'DaysSinceCreation'], axis=1)

    # Drop any rows with missing values and reset the index
    df = df.dropna().reset_index(drop=True)
    
     
    # Drop any duplicate rows
    df = df.drop_duplicates(subset=['NameHash']).reset_index(drop=True)
    
    # Drop age values that are negative, below 18 or above 85 and reset the index
    
    df = df[(df['Age'] >= 18) & (df['Age'] <= 85)].reset_index(drop=True)
    
     # Make all columns title lower case
    df.columns = df.columns.str.lower() 
    
 
    # Return the cleaned DataFrame
    return df