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
    
    def CreatePlanTable(self, verbose=False):
        '''
        Create tPlan, the table that will store each of the simulated districting plans.


        verbose: Mainly for testing, so we can see what's going on when the code is running. Adjust or add more options as needed.
        '''

        self.Connect()
        self.curs.execute("DROP TABLE IF EXISTS tPlan;")

        # Initial part of the sql statement. Pardon the ugly unindent.
        sql = """
CREATE TABLE tPlan (
plan_id TEXT,
vtd TEXT NOT NULL,
dist INTEGER NOT NULL,
PRIMARY KEY(plan_id, vtd)
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
plan_id TEXT,
dist TEXT,
dist_index INTEGER NOT NULL,
seg_score FLOAT NOT NULL);
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
    
    def FillTable(self, table_name, data):
        """Load data from a dataframe to a table, 
        assumes that the columns in the dataframe and the table have the same name"""
        
        print("fill function")
        sql = "INSERT INTO " + table_name + " (" + \
            ",".join([c for c in data.columns]) + \
            ") VALUES (" + ",".join([':' + c for c in data.columns]) + ");"
        
        for row in data.to_dict(orient='records'):
            try:
                self.curs.execute(sql,row)
            except Exception as e:
                print(e)
                print(row)
                return 0
        print("returning fill function")
        return 1
    
    def PrepAssignments(self, file):

        seed  = file.split('_')[3]

        data = pd.read_csv(file)
        data.rename(columns={'Iteration':'vtd'}, inplace=True)

        data_m = data.melt(id_vars='vtd',var_name='iter',value_name='dist')
        data_m["plan_id"] = seed + '_' + data_m["iter"]
        data_m = data_m.drop('iter',axis=1)

        return(data_m)
    
    def MeltScores(self, file, sorted=False):

        seed = file.split('_')[4]

        scores = pd.read_csv(file,names=[1001,1002,1003,1004,1005,1006,1007])

        var_name = 'dist'

        if sorted:
            scores = pd.DataFrame(np.sort(scores.values, axis=1), index=scores.index, columns=scores.columns)

            var_name = 'dist_index'

        scores['plan_id'] = seed
        scores = scores.reset_index().rename(columns = {'index':'iteration'})
        scores = scores.astype({'iteration':str})
        scores['plan_id'] = scores['plan_id'] + '_' + scores['iteration']
        scores = scores.drop('iteration',axis=1)

        scores_m = pd.melt(scores,id_vars=['plan_id'],var_name=var_name,value_name='seg_score')    

        return scores_m
    
    def PrepScores(self, file):

        sorted = self.MeltScores(file, sorted=False)
        unsorted = self.MeltScores(file, sorted=True)

        return sorted.merge(right=unsorted, left_on=['plan_id','seg_score'], right_on=['plan_id','seg_score'])
    
    def LoadPlanFiles(self, directory, verbose=False):
        '''
        Load the contents of file_name into tPlan
        '''

        files = glob(directory + '/' + '*')

        self.Connect()

        for file in files:

            print(file)

            data = self.PrepAssignments(file)

            try:
                print("filling table")
                self.FillTable("tPlan",data)
            except:
                # If anything goes wrong here, print detailed information,
                # rollback all changes in the database since the last commit,
                # close the connection, and return False
                print('\n\nProblem loading:', file)
                self.conn.rollback()
                self.conn.close()
                return False

        # If everything went OK, commit the changes and return True
        print("committing")
        self.conn.commit()
        self.conn.close()
        return True
    
    def LoadScoreFiles(self, directory, verbose=False):
        '''
        Load the contents of file_name into tPlan
        '''

        files = glob(directory + '/' + '*')

        self.Connect()

        for file in files:

            data = self.PrepScores(file)

            try:
                self.FillTable("tScores",data)
            except:
                # If anything goes wrong here, print detailed information,
                # rollback all changes in the database since the last commit,
                # close the connection, and return False
                print('\n\nProblem loading:', file)
                self.conn.rollback()
                self.conn.close()
                return False

            # If everything went OK, commit the changes and return True
            self.conn.commit()
            self.conn.close()
            return True
    
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
