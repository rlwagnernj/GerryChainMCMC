import os 
import shutil
from glob import glob
import time 

RUN   = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.02/run"   
NEXT = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.02/next"    
USED = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.02/used" 

FLAG_DATA = 'RunTwo'

branch = 398
seed = 6050
parallel_path = 'C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/RunGerryChainParallelDefs.py'

while branch < 500:

    num_files = len(os.listdir(RUN))

    if num_files == 0:
        num_files = 1
        n=1
    elif num_files > 10:
        n=10
    else:
        n = num_files

    while num_files > 0: # until RUN is empty

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
            for i in range(num_files):
                file = files[i]
                shutil.move(file, USED)
        print(str(len(files)) + 'in RsUN')
        num_files=num_files-10
        print(branch)

    # move the next files to RUN
    pattern = os.path.join(NEXT, FLAG_DATA + '_assignments' + '*')
    files = glob(pattern)
    for file in files:
        shutil.move(file, RUN)




"""os.system('C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')
os.system('start cmd /k C:/Users/rlwagner01/.conda/envs/gerry/python.exe c:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/hello.py')"""
