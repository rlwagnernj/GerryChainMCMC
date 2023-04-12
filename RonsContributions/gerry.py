import sqlite3
import warnings
import numpy as np
import pandas as pd
from glob import glob
from traceback import print_exc as pe


# This code is meant to start with the output RW has created 
# from the gerrychain code as it is currently formatted.

# The code creates a sqlite database, 
# loads in the results of the gerrychain simulations,
# and should be extended to perform additional manipulations

class GerryDB():
    '''
    This class contains code to work with the gerrychain output from RW,
    get it into a database, and work with the database
    '''

    # Path to the data, relative to where this module is located
    PATH = '../Output/Chain_02.23/next/'
    
    # Dictionary used to convert characters in VTD names that are invalid/undesirable in SQL database column names
    # Keys/values can be reversed to recover the original VTD name 
    CHARS = {'-' : '_hyphen_',
             '.' : '_dot_'}
    
    # Since vtds usually start with numbers, and SQL columns cannot, we insert this string at the beginning of the VTD name
    VTD = 'vtd_'

    def __init__(self):
        # Initialize the list of VTDs. It is assumed they are the same across all files, 
        # so we just grab them from one
        any_file_from_RW = __class__.GetFiles()[0]
        self.GetVTDs(any_file_from_RW, set_as_main_names=True)
        return
    
    def Connect(self):
        self.conn = sqlite3.connect('gerry.db')
        self.curs = self.conn.cursor()
        self.curs.execute("PRAGMA foreign_keys=ON;")
        return
    
    def GetFiles():
        '''
        Static method to return all the file names with simulation results.
        Made this one static so we can quickly check if we're looking in the right directory
        without having to instantiate the class.
        '''
        return glob(__class__.PATH + '*')
    
    def GetLine(self, file, verbose=False):
        '''
        Preview the first line of data in one of the files
        '''
        
        with open(file, 'r') as f:
            for i, line in enumerate(f):
                if i > 0:
                    if verbose: print(line)
                    return line
        return
    
    def GetVTDs(self, file, set_as_main_names = False):
        '''
        Extract the VTD names as is from a RW file.

        Use the SanitizeVTD function to convert them into valid names for the SQL database.

        The expectation is that the names are the same across all files,
        and that we will use the names in the first file we read as the
        column names in the database.

        By default, it just extracts and returns the names. To save them as as the 'master' list
        of names, call with set_as_main_names = True
        '''

        vtds = []
        with open(file, 'r') as f:
            for i, line in enumerate(f):
                if i > 0:
                    vtds.append(line.split(',')[0])
        if set_as_main_names:
            self.vtds = vtds
        return

    def SanitizeVTD(self, vtd):
        '''
        Replace invalid/undesirable characters in VTD name with those defined in CHARS
        '''
        for key, value in self.CHARS.items():
            vtd = vtd.replace(key, value)
        return vtd
    
    def UnsanitizeVTD(self, vtd):
        '''
        Undo the logic in SanitizeVTD and return the original VTD name
        '''
        for key, value in self.CHARS.items():
            vtd = vtd.replace(value, key)
        return vtd
    
    def CreatePlanTable(self, stop_at_column=None, verbose=False):
        '''
        Create tPlan, the table that will store each of the simulated districting plans.

        Before calling this function, self.vtds must be initialized by calling GetVTDs() and passing in one of the files.
        We assume that the VTD names are consistent throughout all the RW files.

        Each table will contain the plan_id in the first column (which is currently just a sequentially assigned inter)
        with the rest of columns containing the inital district_id assigned by RW.

        The table will contain N+1 columns, where N is the number of VTDs in the state

        Other inputs:
            stop_at_column: For testing purposes.  Will only create this many VTD columns.

            verbose: Mainly for testing, so we can see what's going on when the code is running. Adjust or add more options as needed.
        '''

        self.Connect()
        self.curs.execute("DROP TABLE IF EXISTS tPlan;")

        # Initial part of the sql statement. Pardon the ugly unindent.
        sql = """
CREATE TABLE tPlan (
--plan_id INTEGER PRIMARY KEY AUTOINCREMENT,

/* 
commented out the primary key for now, 
if we do have a primary key it should be more meaningful than an autoincrement 
*/
"""
        self.vtd_columns = []

        for i, vtd in enumerate(self.vtds):
            column_name = self.VTD + self.SanitizeVTD(vtd)
            self.vtd_columns.append(column_name)

            sql += column_name + ' INTEGER NOT NULL,\n'

            if stop_at_column is not None:
                if i > stop_at_column:
                    break

        # Strip off the last comma and close the create table statement
        sql = sql[:-2] + ');'

        if verbose: print(sql)
        
        try:
            self.curs.execute(sql)
        except:
            pe()
            print('\nFailed to create tPlan. See the details:')
            if verbose: print(sql)
        
        self.conn.close()
        return
    
    def LoadFile(self, file_name, verbose=False):
        '''
        Load the contents of file_name into tPlan
        '''
        self.Connect()

        try:
            # The INSERT statement.  Using named parameters to ensure that we are not assuming rows line up betwen files.
            sql = 'INSERT INTO tPlan (' + ','.join(self.vtd_columns) + ') VALUES (:' + ',:'.join(self.vtd_columns) + ');'
            if verbose: print(sql)

            # Load the file into a dataframe. Using a pandas dataframe incurs additional overhead (vs an array),
            # but we need to loop over columns rather than rows.

            # Set the index of the dataframe as the column named Iteration to separate it from the rest of the plan
            df = pd.read_csv(file_name, index_col='Iteration')
            
            # Extract the VTD names from the dataframe and append the VTD prefix
            # since SQL columns can't start with digits
            vtd_list = [self.VTD + self.SanitizeVTD(v) for v in list(df.index)]

            # Iterate over each column (plan) in the dataframe
            for column, district_id in df.items():

                # Extract the values from this column
                district_id = district_id.values

                # Keep the last digit only (valid for AL, since there are only 7 districts. Need to adjust if a state has more than 10 districts)
                district_id = [int(str(d)[-1]) for d in district_id]

                # Make a dictionary with the VTDs as keys, and their district_id as values
                params = dict(zip(vtd_list, district_id))

                # Insert this row into the database
                self.curs.execute(sql, params)
        except:
            # If anything goes wrong here, print detailed information,
            # rollback all changes in the database since the last commit,
            # close the connection, and return False
            pe()
            print('\n\nProblem loading:', file_name)
            print('\n\nProblem occured at column:', column)
            self.conn.rollback()
            self.conn.close()
            return False

        # If everything went OK, commit the changes and return True
        self.conn.commit()
        self.conn.close()
        return True
    
    def LoadFiles(self, stop_after=0, verbose=True):
        ''''
        Load the simulation results into the database.

        For testing purposes, this currently only loads one file,
        and then raises a warning to alert RW that the code isn't complete
        '''

        file_names = __class__.GetFiles()

        for i, file in enumerate(file_names):
            print('Loading file:', file)
            exit_code = self.LoadFile(file)
            print("""
            Hi Rebecca,

            Currently only loading 1 file, for testing purposes! 

            Modify gerry.GerryDB.LoadFiles() when you're ready to run it for real.
            
            Best,
            -Ron
            """)

            if i==stop_after: return exit_code
        return exit_code
    
    def RelabelDistricts(self):
        '''
        Logic to relabel districts should go here
        Right now, we'll just grab a row or two and do something to them
        '''
        self.Connect()

        # Queue up the data
        all_plans = self.curs.execute("SELECT * FROM tPlan;")

        # Get the first plan from the top of the stack
        current_plan = all_plans.fetchone()

        # Just doing something to it as an example, 
        # i.e. adding 1 to all the numbers
        
        # The actual logic of how to relabel them would go here:
        #
        adjusted_plan = np.array(current_plan) + 1
        print('The current plan in our first row was: ', current_plan)
        print('\n And now it is:', tuple(adjusted_plan))
        #
        #
        ##########################################################

        # Now move on to the next plan...
        #  You could run a loop until all_plans.fetchone() returns an empty set
        #  and write the results to a new table

        # Idea is that we are editing them one by one, without having them all loaded in memory at the same time

        self.conn.close()
        return

    def RunQuery(self, sql, params=None):
        '''
        Convenience method for running queries on the database.

        Also ensures that we don't leave a stray connection open when running queries.
        '''
        self.Connect()
        try:
            if params is not None:
                results = pd.read_sql(sql, self.conn, params=params)
            else:
                results = pd.read_sql(sql, self.conn)
        except:
            pe()
            print('\n\nThere was a problem running the query...')
            print('\nThe query was:\n', sql)
            print('\nThe parameters passed were:\n', params)
            results = None
        self.conn.close()
        return results