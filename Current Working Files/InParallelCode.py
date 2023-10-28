import os
import shutil
from glob import glob
import pandas as pd


from Chain_Class import Chain
from Create_Chain import *
from THIS_rungerrychain import RunGerryChain

############################################################ 
# Variables to Update #
# No code beyond this section needs to be changed, unless you would like to alter the updaters/constraints #

# Data Storage #
RUN = ''
NEXT = ''
USED = ''
META_DATA = ''
FLAG_DATA = ''

# Creating Chain #
SHP = ''
STEPS = 2000
GROUPING_ASSIGNMENT_COLUMN = ''
POP_DEVIATION = .05

# Running Chain #
GEOGRAPHIC_AREA_COLUMN = '' 
POPULATION_COLUMN = ''

# Parallel Variables #
COUNT = 0 # COUNT the number of branches already run
TOTAL_BRANCHES = '' # total number of branches to run. total iter = total branches * chain.steps
SEED = '' # SEED to start with

# Ensure output directories exist
for directory in [USED, RUN, NEXT, META_DATA]:
    if not os.path.exists(directory):
        os.mkdir(directory)

############################################################ 
# Updaters for Chain #
# To add an updater, define the function here AND add it to the UPDATERS list #

def split_counter(partition):
    count_splits = 0
    for item in partition["county_info"]:
        count_splits += len(partition["county_info"][item][2]) - 1
    return count_splits

def num_split_counties(partition):
    counties_with_splits = 0
    for item in partition["county_info"]:
        if len(partition["county_info"][item][2]) > 1:
            counties_with_splits += 1
    return counties_with_splits

def oversplit_counties(partition):
    oversplits = 0
    for item in partition["county_info"]:
        if len(partition["county_info"][item][2]) > 2:
            oversplits += 1
    return oversplits

def bad_incumbents(partition):
    num_problems = 0
    for part in partition.parts:
        if partition["incumbents"][part] > 1:
            num_problems += 1
    return num_problems

def subpop_over_threshold(partition):
    count_over = 0
    alpha = 0.5# Arbitrarily defined threshold
    for dist in partition['population']:
        # whatever this first term is needs to be an updater in my updaters so the constraint validator can look for it
        if partition['subpop'][dist] / partition['population'][dist] >= alpha:
            count_over += 1
    return count_over

UPDATERS = [split_counter, num_split_counties, oversplit_counties, bad_incumbents, subpop_over_threshold]

############################################################
# Create Original Chain #
# This object has updaters but no constraints #

chain = CreateChain(steps=STEPS, shp_file=SHP, grouping_assignment_column=GROUPING_ASSIGNMENT_COLUMN, updaters=UPDATERS, pop_deviation=POP_DEVIATION)

############################################################
# Add constraints #
# I haven't figured out a better way to do this. See Create_Chain.py #

pop_constraint = constraints.within_percent_of_ideal_population(chain.get_current_partition(), chain.pop_deviation)

county_limit = constraints.UpperBound(
    lambda p: split_counter(p),
    3*split_counter(chain.get_current_partition()))

contiguity = constraints.contiguous

CONSTRAINTS = [pop_constraint,county_limit,contiguity]

AddConstraints(chain=chain,constraints_list=CONSTRAINTS)

############################################################
# file name functions #

# moved to run gerry chain

############################################################
# Remaining iterations #

while COUNT < TOTAL_BRANCHES: # while there are stil branches to run

    num_files = len(os.listdir(RUN)) # the number of files in RUN. determines how many parallel terminals to open. 

    if num_files == 0: # if this is the starting point and we have no files to start with
        
        # Get the output file names using seed history
        chain_file_name = FLAG_DATA + '_assignments_' + seed_hist(cur_seed=SEED, seed_filename=None) + '_' + time_stamp()

        # Run one branch in a terminal 
        cmd = ''
        RunGerryChain(chain_obj=chain, seed=SEED, geographic_area_column= GEOGRAPHIC_AREA_COLUMN, population_column= POPULATION_COLUMN, output_path, show_progress=True)

    while num_files > 0: # until run is empty, aka we have used all the files 

        if num_files > 10: # need to know how many terminals to open
            num_terminals = 10
        else:
            num_terminals = num_files

        for i in range(num_terminals): # for the first 10 files in run

            # identify the file to start with 
            file = os.listdir(RUN)[i]

            ############################################################
            # update the initial parition of the chain based on the file selected #

            new = pd.read_csv(file) # read the file
            file.set_index('Iteration',inplace=True) # reset the index
            new = file.iloc[:,-1:] # get the last iteration

            # make a new gdf 
            new_gdf = chain.gdf.merge(new,left_on='VTD',right_index=True)
            new_gdf.drop(labels='cd_117',axis=1,inplace=True)
            new_gdf.rename(columns={str(chain.steps):GROUPING_ASSIGNMENT_COLUMN},inplace=True)

            # update the partition
            chain.update_partition(new_gdf=new_gdf, grouping_assignment_column=GROUPING_ASSIGNMENT_COLUMN)

            ############################################################

            cmd = '' # whatever command is needed
            # RUN AN ITERATION HERE
            RunGerryChain(chain_obj=chain, seed=SEED, geographic_area_column= GEOGRAPHIC_AREA_COLUMN, population_column= POPULATION_COLUMN, output_path, show_progress=True)

            SEED += 1 # increment SEED

            cmd = ''
            # RUN SECOND ITERATION HERE

            SEED += 1 # increment SEED

            COUNT += 2 # increment COUNT twice for each branch we ran

        # INSERT WAIT LINE HERE
        os.system('start cmd /c taskkill /IM WindowsTermal.exe') # kill all terminals 

        # move used files to USED
        
        num_files -= 10 
    
    # now we've used all the files in run, they are all in used. the next files are stored in NEXT

    # move the next files to RUN
    





