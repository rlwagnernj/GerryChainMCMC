
from RunGerryChainDefs_v4 import * 

from glob import glob
from datetime import datetime as dt
import geopandas as gpd

import sys
import os
import csv

######################################################################################################################################################################################################

FLAG_DATA  = 'MyResults'     # File name header to identify only simulation data files (nothing else should start with this string)
PATH_OUT   = 'out'           # Directory to save the output
SHP = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/State Shp Files/AL/Alabama_VTD_District_Intersection.shp' # file path to shp file 
VTD = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/Agregate VTD Demographic Data/AL.csv'
STEPS = 10

######################################################################################################################################################################################################

def get_files():
    pattern = os.path.join(PATH_OUT, FLAG_DATA + '_assignments' + '*')
    files = glob(pattern)
    return files

def get_recent_files():
    '''Get the most recently used files (judged by how long the sequence of seeds in the filename is)'''
    f = np.array(get_files())
    try:
        counts = np.array([fi.count('.') for fi in f])
        idx = counts == counts.max()
        return list(f[idx])
    except:
        return None

def time_stamp():
    return str(dt.now()).replace(' ', '-').replace(':', '-').replace('.', '-')

def init(graph):
    return InitializeGerryChain(graph=graph) 

def seed_hist(cur_seed, seed_filename):

    if seed_filename is not None:
        # Get the random seed history from the filename
        seed_hist = seed_filename.split('_')[2]
        # Append the current seed to the seed history
        seed_hist = seed_hist + '.' + str(cur_seed)
    else:
        seed_hist = str(cur_seed)

    return seed_hist

def save_output(chain_list, metadata_list, cur_seed, seed_hist):
    
    # Build the new filename
    chain_file_name = FLAG_DATA + '_assignments_' + seed_hist + '_' + time_stamp()
    meta_file_name = FLAG_DATA + '_metadata_' + seed_hist + '_' + time_stamp()

    chain_file_name = os.path.join(PATH_OUT, chain_file_name)
    meta_file_name = os.path.join(PATH_OUT, meta_file_name)

    chain_df = pd.DataFrame(chain_list)
    with open(chain_file_name, 'x', encoding='utf8', newline='') as output_file:
        transposed_csv = chain_df.T
        transposed_csv.to_csv(output_file, sep=',', encoding='utf-8', header=False)

    meta_df = pd.DataFrame(metadata_list)
    with open(meta_file_name, 'x', encoding='utf8', newline='') as output_file:
        transposed_meta = meta_df.T
        transposed_meta.to_csv(output_file, sep=',', encoding='utf-8', header=False)

    return chain_file_name

######################################################################################################################################################################################################

def run_branch(graph, partition, proposal, constraint, steps, seed_hist, seed = int, print_iterations = True):
    
    # run a chain
    chain_list, metadata_list = RunChain(graph = graph, partition = partition, proposal = proposal, constraint = constraint, steps = steps, seed = seed, print_iterations = print_iterations)
    # save the output
    chain_file_name = save_output(chain_list = chain_list, metadata_list = metadata_list, cur_seed=seed, seed_hist=seed_hist)
    return chain_file_name

def get_next_partition(shp, i = int):

    gdf = gpd.read_file(SHP)
    gdf["incumbent"] = gdf["incumbent"].fillna(0)

    files = get_recent_files()

    if files is None:
        return 
    else:
        f = files[i]

        with open(file=f) as file:
            reader = csv.reader(file)
            last_assignment = [row[-1] for row in reader]

        with open(file=f) as file:
            reader = csv.reader(file)
            vtds = [row[0] for row in reader]

        last_iter = pd.DataFrame(vtds).merge(right=pd.DataFrame(last_assignment), right_index=True, left_index=True).drop(labels=0, axis=0).rename({"0_y":"cd_117"}, axis=1)
        return gdf.merge(right=last_iter, left_on='vtd_cd_117', right_on='0_x').drop(columns=['cd_117_x','vtd_cd_117']).rename({'0_x':'vtd_cd_117','cd_117_y':'cd_117'}, axis=1)

def segregation_output(file, seed, vtd_data, seed_hist):

    #pattern = os.path.join(PATH_OUT, FLAG_DATA + '_assignments_' + str(seed) + '*')
    #file = glob(pattern)

    data = pd.read_csv(file, index_col="Iteration", dtype=str)
    data.index = data.index.map(lambda x: x[:-6])

    districts = data['1'].unique()
    segregation_frame = pd.DataFrame(index=districts,columns=list(range(1,STEPS)))

    for i in list(data.columns):
            score_list = []
            for district in districts:
                dist_id = data[i] == district
                vtd_list = list(data[dist_id].index)
                seg_score = CalculateSegregation(data_file_path=vtd_data,vtds=vtd_list)
                score_list.append(seg_score)
            segregation_frame[int(i)] = score_list

    seg_file_name = FLAG_DATA + '_segregation_' + seed_hist + '_' + time_stamp()
    seg_file_name = os.path.join(PATH_OUT, seg_file_name)

    with open(seg_file_name, 'x', encoding='utf8', newline='') as output_file:
        transposed_seg = segregation_frame.T
        transposed_seg.to_csv(output_file, sep=',', encoding='utf-8', header=False)

######################################################################################################################################################################################################

if __name__ == "__main__":

    SEED, FILE_NUM = int(sys.argv[1]), int(sys.argv[2]) 

    # Make sure the output directory exists, create it if not 
    if not os.path.exists(PATH_OUT):
        os.mkdir(PATH_OUT)

    f = get_recent_files()

    if f is None:
        GDF = None
        graph = MakeGraph(SHP, GDF)
        graph, partition, proposal, constraint = init(graph)
        seed_hist_str = seed_hist(cur_seed=SEED, seed_filename=None)
    else:
        GDF = get_next_partition(shp = SHP, i = FILE_NUM) # get the gdf to use
        SHP = None
        graph = MakeGraph(SHP, GDF)
        seed_hist_str = seed_hist(cur_seed=SEED, seed_filename=f[FILE_NUM])
        graph, partition, proposal, constraint = init(graph)
        
    chain_file_name = run_branch(graph = graph, partition=partition, proposal=proposal, constraint=constraint, steps=STEPS, seed=SEED, seed_hist = seed_hist_str, print_iterations=True)

    segregation_output(file=chain_file_name, seed = SEED, vtd_data = VTD, seed_hist=seed_hist_str)
        
    print(0)
    print("Next Seed: " + str(SEED + 1))
