import os 
import shutil
from glob import glob
import time 

RUN   = "C:/Users/rlwagner01/Desktop/run"  
NEXT = "C:/Users/rlwagner01/Desktop/next"    
USED = "C:/Users/rlwagner01/Desktop/used"

FLAG_DATA = 'RunOne'

branch = 417
seed = 508
parallel_path = 'C:/Users/rlwagner01/.conda/envs/gerry/python.exe C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/py_Files/RunGerryChain_Parallel.py'

while branch < 500:

    num_files = len(os.listdir(RUN))

    if num_files > 10:
        n=10
    else:
        n = num_files

    while num_files > 0: # until RUN is empty

        for i in range(n):

            cmd = 'start cmd /k ' + parallel_path + ' ' + str(seed) + ' ' + str(i)
            os.system(cmd)

            seed+=1

            cmd = 'start cmd /k ' + parallel_path + ' ' + str(seed) + ' ' + str(i)
            os.system(cmd)

            seed+=1

            branch +=2

        time.sleep(2000)
        os.system('taskkill /IM WindowsTerminal.exe')

        # move the used files to USED
        pattern = os.path.join(RUN, FLAG_DATA + '_assignments' + '*')
        files = glob(pattern)
        for i in range(n):
            file = files[i]
            shutil.move(file, USED)

        num_files=num_files-10

    # move the next files to RUN
    pattern = os.path.join(NEXT, FLAG_DATA + '_assignments' + '*')
    files = glob(pattern)
    for file in files:
        shutil.move(file, RUN)




"""os.system('C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')"""
