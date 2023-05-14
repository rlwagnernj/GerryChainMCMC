
from RunGerryChainDefs_v5 import * 

from glob import glob
from datetime import datetime as dt
import geopandas as gpd

import sys
import os
import csv

######################################################################################################################################################################################################

FLAG_DATA  = 'RunThree'     # File name header to identify only simulation data files (nothing else should start with this string)
RUN   = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/run"   
NEXT = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/next"    
USED = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/used" 
META =  "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/meta" 
SEG = "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/seg" 
BVAP =  "C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Output/Chain_05.10/bvap" 
SHP = 'C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Data/State Shp Files/AL/Alabama_VTD_District_Intersection.shp' # file path to shp file 
VTD = 'C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Data/Agregate VTD Demographic Data/AL.csv'
BVAP_VTD = 'C:/Users/rlwagner01/Documents/GitHub/GerryChainMCMC/Data/bvap_vtds.csv'
STEPS = 2000

######################################################################################################################################################################################################

def get_files():
    pattern = os.path.join(RUN, FLAG_DATA + '_assignments' + '*')
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

def init(seed, graph):
    return InitializeGerryChain(seed=seed, graph=graph) 

def seed_hist(cur_seed, seed_filename):

    if seed_filename is not None:
        # Get the random seed history from the filename
        seed_hist = seed_filename.split('_')[3]
        # Append the current seed to the seed history
        seed_hist = seed_hist + '.' + str(cur_seed)
    else:
        seed_hist = str(cur_seed)

    return seed_hist

def save_output(chain_list, metadata_list, seed_hist):
    
    # Build the new filename
    chain_file_name = FLAG_DATA + '_assignments_' + seed_hist + '_' + time_stamp()
    meta_file_name = FLAG_DATA + '_metadata_' + seed_hist + '_' + time_stamp()

    chain_file_name = os.path.join(NEXT, chain_file_name)
    meta_file_name = os.path.join(META, meta_file_name)

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
    chain_file_name = save_output(chain_list = chain_list, metadata_list = metadata_list, seed_hist=seed_hist)
    return chain_file_name

def get_next_partition(shp, i = int):

    gdf = gpd.read_file(shp)
    gdf["incumbent"] = gdf["incumbent"].fillna(0)

    files = get_files()

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

def segregation_output(file, vtd_data, seed_hist):

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
    seg_file_name = os.path.join(SEG, seg_file_name)

    with open(seg_file_name, 'x', encoding='utf8', newline='') as output_file:
        transposed_seg = segregation_frame.T
        transposed_seg.to_csv(output_file, sep=',', encoding='utf-8', header=False)

def bvap_output(file, vtd_data, seed_hist):

    bvap_scores = list()

    data = pd.read_csv(file)
    vtd_data = pd.read_csv(vtd_data)
    districts = data['1'].unique()
    data = data.set_index('Iteration')
    data.index = data.index.map(lambda x: x[:-6])

    for column in data.columns:
        score_list = []
        for district in districts:
            vtds = list(data[data[column] == district].index)
            district_data = vtd_data[vtd_data['block_vtda'].isin(vtds)]
            bvap = district_data.sum()['black_combo_18']/district_data.sum()['total_18'] 
            score_list.append(bvap)
        bvap_scores.append(score_list)

    frame = pd.DataFrame(bvap_scores, columns=districts)
    bvap_file_name = FLAG_DATA + '_bvap_' + seed_hist + '_' + time_stamp()
    bvap_file_name = os.path.join(BVAP, bvap_file_name)

    with open(bvap_file_name, 'x', encoding='utf8', newline='') as output_file:
            frame.to_csv(output_file, sep=',', encoding='utf-8', header=True, index=False)


######################################################################################################################################################################################################

if __name__ == "__main__":

    SEED, FILE_NUM = int(sys.argv[1]), int(sys.argv[2]) 

    # Make sure the output directories exists, create if not 
    for directory in [USED, RUN, NEXT, META, SEG, BVAP]:
        if not os.path.exists(directory):
            os.mkdir(directory)

    f = get_files()

    if f == []:
        GDF = None
        graph = MakeGraph(SEED, SHP, GDF)
        graph, partition, proposal, constraint = init(SEED, graph)
        seed_hist_str = seed_hist(cur_seed=SEED, seed_filename=None)
    else:
        GDF = get_next_partition(shp = SHP, i = FILE_NUM) # get the gdf to use
        SHP = None
        graph = MakeGraph(SEED, SHP, GDF)
        seed_hist_str = seed_hist(cur_seed=SEED, seed_filename=f[FILE_NUM])
        graph, partition, proposal, constraint = init(SEED, graph)
        
    chain_file_name = run_branch(graph = graph, partition=partition, proposal=proposal, constraint=constraint, steps=STEPS, seed=SEED, seed_hist = seed_hist_str, print_iterations=True)

    segregation_output(file=chain_file_name, vtd_data = VTD, seed_hist=seed_hist_str)
    bvap_output(file=chain_file_name,vtd_data = BVAP_VTD, seed_hist=seed_hist_str)
        
    print(0)
    #print("Next Seed: " + str(SEED + 1)) 
