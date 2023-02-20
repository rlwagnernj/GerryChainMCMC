from RunGerryChainDefs_v2 import *
import csv 
import geopandas as gpd
import pandas as pd

""" shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/State Shp Files/AL/Alabama_VTD_District_Intersection.shp'
al_gdf = gpd.read_file(shp_file) # returns a GeoDataFrame from file
al_gdf["incumbent"] = al_gdf["incumbent"].fillna(0)

with open(file='/Users/rebeccawagner/Desktop/Csv_100_200_300_400_500_600_700_800_900_100000all') as f:
    reader = csv.reader(f)
    my_list = [row[-1] for row in reader]

with open(file='/Users/rebeccawagner/Desktop/Csv_100_200_300_400_500_600_700_800_900_100000all') as f:
    reader = csv.reader(f)
    list_1 = [row[0] for row in reader]

last_iter = pd.DataFrame(list_1).merge(right=pd.DataFrame(my_list), right_index=True, left_index=True)
last_iter = last_iter.drop(labels=0, axis=0)

last_iter = last_iter.rename({"0_y":"cd_117"}, axis=1)

final = al_gdf.merge(right=last_iter, left_on='vtd_cd_117', right_on='0_x')
final = final.drop(columns=['cd_117_x','vtd_cd_117']).rename({'0_x':'vtd_cd_117','cd_117_y':'cd_117'}, axis=1) """

shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/State Shp Files/AL/Alabama_VTD_District_Intersection.shp'
n = 20
vtd_data = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/Agregate VTD Demographic Data/AL.csv'
output_path = '/Users/rebeccawagner/Desktop/'

(graph, chain, al_gdf) = GerryChainSetUp(Input_Shp = shp_file, Total_Steps = n)

(chain_list, metadata_list, part_dict, seg_output) = RunGerryChain(GDF = al_gdf, 
                                                                    Graph = graph, 
                                                                    Chain = chain,
                                                                    VTD_Data = vtd_data,
                                                                    Checkpoint = 5,
                                                                    Print_Iterations = True,
                                                                    Output_Csv = output_path + "csv",
                                                                    Output_Metadata = output_path + 'meta',
                                                                    Output_Seg = output_path + 'seg')