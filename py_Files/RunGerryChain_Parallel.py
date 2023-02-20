
from RunGerryChainDefs_v4 import * 

from glob import glob
from datetime import datetime as dt

import sys
import os

######################################################################################################################################################################################################

#FLAG_DATA  = 'MyResults'     # File name header to identify only simulation data files (nothing else should start with this string)
#PATH_OUT   = 'out'           # Directory to save the output
#INITIAL_SEED = 1

# Input Files
#shp = '' # file path to shp file 

######################################################################################################################################################################################################

def get_files():
    pattern = os.path.join(PATH_OUT, FLAG_DATA + '*')
    files = glob(pattern)
    return files

def get_recent_files():
    '''Get the most recently used files (judged by how long the sequence of seeds in the filename is)'''
    f = np.array(get_files())
    counts = np.array([fi.count('.') for fi in f])
    idx = counts == counts.max()
    return list(f[idx])

def time_stamp():
    return str(dt.now()).replace(' ', '-').replace(':', '-').replace('.', '-')

def init(graph):
    return InitializeGerryChain(graph=graph) 


def save_output(chain_list, metadata_list, cur_seed, seed_filename=None):
    
    if seed_filename is not None:
        # Get the random seed history from the filename
        seed_hist = seed_filename.split('_')[1]
        # Append the current seed to the seed history
        seed_hist = seed_hist + '.' + str(cur_seed)
    else:
        seed_hist = str(cur_seed)

    # Build the new filename
    chain_file_name = FLAG_DATA + '_assignments_' + seed_hist + '_' + time_stamp()
    meta_file_name = FLAG_DATA + '_metadata_' + seed_hist + '_' + time_stamp()

    chain_file_name = os.path.join(PATH_OUT, chain_file_name)
    meta_file_name = os.path.join(PATH_OUT, meta_file_name)

    np.savetxt(chain_file_name, chain_list, fmt='%s') # Do I need to change the form of the list?
    np.savetxt(meta_file_name, metadata_list, fmt='%s')

######################################################################################################################################################################################################

def run_branch(graph, partition, proposal, constraint, steps, seed = int, print_iterations = True):
    
    # run a chain
    chain_list, metadata_list = RunChain(graph = graph, partition = partition, proposal = proposal, constraint = constraint, steps = steps, seed = seed, print_iterations = print_iterations)
    # save the output
    save_output(chain_list = chain_list, metadata_list = metadata_list, cur_seed=seed)

def get_next_partition():

    files = get_recent_files()
    num_files = len(files)

    for i in range(num_files):
        f = files[i]

        # now we pull the last assignment from each map
        # and make the new partition

######################################################################################################################################################################################################

if __name__ == "__main__":
    '''pass None for shp if not first branch'''

    FLAG_DATA, PATH_OUT, SEED, SHP = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

    # Make sure the output directory exists, create it if not 
    if not os.path.exists(PATH_OUT):
        os.mkdir(PATH_OUT)

    gdf = None

    graph = MakeGraph(SHP, gdf)
    graph, partition, proposal, constraint = init(graph)
    run_branch(graph = graph, partition=partition, proposal=proposal, constraint=constraint, steps=100, seed=SEED, print_iterations=True)