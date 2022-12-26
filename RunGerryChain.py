from RunGerryChainDefs import *


shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/Alabama shp files/Alabama_VTD_District_Intersection.shp'
n = 1000
vtd_data = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/Agregate VTD Demographic Data/AL.csv'
output_path = '/Users/rebeccawagner/Desktop/AL_GerryOutput/'

(graph, chain) = GerryChainSetUp(Input_Shp = shp_file, Total_Steps = n)

(chain_list, metadata_list, part_dict, seg_output) = RunGerryChain(Graph = graph, Chain = chain,
                                                                    VTD_Data = vtd_data,
                                                                    Print_Iterations = True,
                                                                    Output_Csv = output_path + "csv",
                                                                    Output_Metadata = output_path + 'meta',
                                                                    Output_Seg = output_path + 'seg')