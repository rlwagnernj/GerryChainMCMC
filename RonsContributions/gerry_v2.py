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
    NEXT_PATH = '../Output/Chain_02.23/next/'
    USED_PATH = '../Output/Chain_02.23/used/'
    SCORES_PATH = '../Output/Chain_02.23/scores/'

    def __init__(self):
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
        return glob(__class__.NEXT_PATH + '*') + glob(__class__.USED_PATH + '*')
         
    
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
    
    
    def CreatePlanTable(self, stop_at_column=None, verbose=False):
        '''
        Create tPlan, the table that will store each of the simulated districting plans.

        Before calling this function, self.vtds must be initialized by calling GetVTDs() and passing in one of the files.
        We assume that the VTD names are consistent throughout all the RW files.

        Other inputs:
            stop_at_column: For testing purposes.  Will only create this many VTD columns.

            verbose: Mainly for testing, so we can see what's going on when the code is running. Adjust or add more options as needed.
        '''

        self.Connect()
        self.curs.execute("DROP TABLE IF EXISTS tPlan;")

        # Initial part of the sql statement. Pardon the ugly unindent.
        sql = """
CREATE TABLE tPlan (
plan_id TEXT,
vtd TEXT NOT NULL,
iter INTEGER NOT NULL,
dist INTEGER NOT NULL,
PRIMARY KEY(vtd, iter)
);
""" 

        if verbose: print(sql)
        
        try:
            self.curs.execute(sql)
        except:
            pe()
            print('\nFailed to create tPlan. See the details:')
            if verbose: print(sql)
        
        self.conn.close()
        return
    
    def CreateScoresTable(self, verbose=False):

        self.Connect()
        self.curs.execute("DROP TABLE IF EXISTS tScores;")

        # Initial part of the sql statement. Pardon the ugly unindent.
        sql = """
CREATE TABLE tScores (
plan_id TEXT REFERENCES tPlan(plan_id),
dist INTEGER NOT NULL,
seg_score FLOAT NOT NULL,
bvap FLOAT);
""" 

        if verbose: print(sql)
        
        try:
            self.curs.execute(sql)
        except:
            pe()
            print('\nFailed to create tScores. See the details:')
            if verbose: print(sql)
        
        self.conn.close()
        return
    
    def CreateVTDTable(self, verbose=False):

        self.Connect()
        self.curs.execute("DROP TABLE IF EXISTS tVtd;")

        # Initial part of the sql statement. Pardon the ugly unindent.
        sql = """
CREATE TABLE tVtd (
vtd TEXT NOT NULL REFERENCES tPlan(vtd),
incumbent TEXT NOT NULL,
geography BLOB);
""" 

        if verbose: print(sql)
        
        try:
            self.curs.execute(sql)
        except:
            pe()
            print('\nFailed to create tVtd. See the details:')
            if verbose: print(sql)
        
        self.conn.close()
        return
    
    def prep(file):

        seed = file.split('_')[3]

        data = pd.read_csv(file)
        data['Iteration'] = [x[:-6] for x in data['Iteration']]
        data.rename(columns={'Iteration':'vtd'}, inplace=True)

        data_m = data.melt(id_vars='vtd',var_name='iter',value_name='dist')
        data_m["plan_id"] = seed + '_' + data_m["iter"]

        return(data_m)
    
    def LoadFile(self, file_name, verbose=False):
        '''
        Load the contents of file_name into tPlan
        '''
        self.Connect()

        for file in self.GetFiles():

            data = self.prep_assignments(file)

            try:
                # The INSERT statement.  Using named parameters to ensure that we are not assuming rows line up betwen files.
                sql = 'INSERT INTO tPlan (' + ','.join(data.columns) + ') VALUES (:' + ',:'.join(self.vtd_columns) + ');'
                if verbose: print(sql)

                params = dict(zip(vtd_list, district_id))

                # Insert this row into the database
                self.curs.execute(sql, params)
            except:
                # If anything goes wrong here, print detailed information,
                # rollback all changes in the database since the last commit,
                # close the connection, and return False
                pe()
                print('\n\nProblem loading:', file)
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