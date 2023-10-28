from Chain import Chain

from RunGerryChainDefs_v6_class import *

import os 
import shutil
from glob import glob
import time 


###############################################################################################
# Edit these variables to start a chain #

FLAG_DATA = '' # File name header to identify only simulation data files (nothing else should start with this string)
OUTPUT_DIR = '' # Directory where you want all your output folders stored. This directory must already exist. 
SHP_FILE = '' # points to shp file with geometry boundaries and assignments (ex vtds to legeslative district)
SEED = 0 # set seed for reproducable results

branch = 0 # how many files do you want to end with?
seed = 0 # what seed do you want to start with? (numerical). 

# path to ParallelDefs.py which actually runs the chain#
parallel_path = ''

###############################################################################################

chain = Chain(flag=FLAG_DATA, shp_file = SHP_FILE, output_dir=OUTPUT_DIR) # create Chain instance

while branch < 500:

    files = chain.get_run_files()

    if files != []: # if there are files in run
        num_files = len(files) # num_files = the # of files


    else: # if there are no files in run, then this is the very beginning of the chain

        initial_graph = MakeGraph(seed = seed, shp = chain.shp_file) # make initial graph object & partition
        # initialize updaters 


        num_files = 1 # otherwise set to 1 so that the while loop will execute once

    while num_files > 0: # until RUN is empty 

        if num_files > 10: # if there are more than 10 files in run we need to only run the first 10 of them
            n = 10 # so we set the number of files to RUN n = 10 
        else:
            n = num_files # if there are 10 or fewer files, we can run them all at once
    
        for i in range(n):

            ### run one branch with the first seed ###


            #cmd = 'start cmd /k python '  + parallel_path + ' ' + str(seed) + ' ' + str(i)
            #os.system(cmd)

            seed+=1
            
            cmd = 'start cmd /k python ' + parallel_path + ' ' + str(seed) + ' ' + str(i)
            os.system(cmd)

            seed+=1

            branch +=2

        print("waiting...")
        time.sleep(3000)
        os.system('start cmd /c taskkill /IM WindowsTerminal.exe')

        # move the used files to USED
        pattern = os.path.join(RUN, FLAG_DATA + '_assignments' + '*')
        files = glob(pattern)
        print(str(len(files)) + 'in RUN')
        if files != []:
            for i in range(n):
                file = files[i]
                shutil.move(file, USED)

    
        print(str(len(files)) + 'in RUN')
        print("branch number " + str(branch))
        num_files=num_files-10

    # move the next files to RUN
    pattern = os.path.join(NEXT, FLAG_DATA + '_assignments' + '*')
    files = glob(pattern)
    for file in files:
        shutil.move(file, RUN)





"""os.system('C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')"""
