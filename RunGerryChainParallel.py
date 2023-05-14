import os 
import shutil
from glob import glob
import time 

RUN   = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/run"   
NEXT = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/next"    
USED = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/used" 

for directory in [USED, RUN, NEXT]:
        if not os.path.exists(directory):
            os.mkdir(directory)

FLAG_DATA = 'RunThree'

branch = 0
seed = 10000
parallel_path = 'C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/RunGerryChainParallelDefs.py'

while branch < 500:

    pattern = os.path.join(RUN, FLAG_DATA + '_assignments' + '*')
    files = glob(pattern)
    if files != []:
        num_files = len(os.listdir(RUN))
    else:
        num_files = 1

    while num_files > 0: # until RUN is empty

        if num_files > 10:
            n=10
        else:
            n = num_files
    
        for i in range(n):

            cmd = 'start cmd /k python '  + parallel_path + ' ' + str(seed) + ' ' + str(i)
            os.system(cmd)

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
