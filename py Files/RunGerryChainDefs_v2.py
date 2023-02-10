
######################################################################################################################################################################################################
# IMPORT MODULES #
######################################################################################################################################################################################################

from codecs import ignore_errors # "Implements the 'ignore' error handling, which ignores malformed data and continues"
from mimetypes import init # "Converts between a filename or URL and the MIME (media) type associated with the filename extension"
                        # "Initialize the internal data structures. If given, files must be a sequence of file names which should be used to augment the default type map"
                        # "MIME mappings specify how a statis file should be interpreted by mapping file extensions to MIME types"
from re import sub # "Reuglar expression matching operations" 
                # "Return the string obtained by replacing the leftmost non-overlapping occurrences of pattern in string by rhe replacement repl"
import os.path # Work with pathnames to files
import json # JSON encoder & decoder
from functools import partial # "Functions that act on or return other functions - any callable object can be treated as a function"
                            # "Return a new partial object which when called will behave like func called with the positional arguments args and keyword arguements keywords"

from gerrychain import (GeographicPartition, Partition, Graph, MarkovChain,
                        proposals, updaters, constraints, accept, Election)
from gerrychain.updaters import Tally, cut_edges
from gerrychain.proposals import recom

import pandas as pd
import numpy as np
import time

# Additional modules for importing shapefile stuff
import geopandas as gpd # "Extends the datatypes used by pandas to allow spatial operations on geometric types. Geometric operations are performed by shapley"
from scipy.stats import beta


def GerryChainSetUp(Input_Shp = '', Total_Steps = 0, Input_Gdf = None):
    """Function to run the MCMC algorithm
        Input_Shp = string path to shp file & associated data
        Total_Step = number of iterations to run (int)
        Input_Gdf = string path to gdf if not starting from 0
        Returns (graph, chain) to pass into RunGerryChain"""

    ######################################################################################################################################################################################################
    # RELEVANT FILES #
    ######################################################################################################################################################################################################

    if Input_Gdf is None:
        # Path to the source file of your vtds
        # If using a shapefile, point to .shp. All connected shapefiles you want to use must be in same directory
        input_file = Input_Shp

        #######################################################################################################################################################################################################
        # MAKE THE GRAPH OBJECT #
        #######################################################################################################################################################################################################

        # I first convert to geodataframe in case anything needs to be cleaned up.
        # Originally there were some columns I needed to get rid of and others I needed to clean up, but this may no longer be the case

        al_gdf = gpd.read_file(input_file) # returns a GeoDataFrame from file
        al_gdf["incumbent"] = al_gdf["incumbent"].fillna(0) # Clean up data. Make sure that if there are no incumbents, it's 0. 

        graph = Graph.from_geodataframe(al_gdf, ignore_errors=True) # Saving info from .json file as graph object
                                                                    # The areas of the polygons are included as node attributes (key area). Shared perimeter of neighboring polygons 
                                                                        # are included as edge attributes (key shared_perim)
                                                                    # key boundry_node = T/F & key boundary_perim on boundry polygons
                                                                    # Is a geodataframe a .geojson? Need more clarity on file types. Pointing to the .shp gets the associated files?
                                                                    # Why are we ignoring errors? What makes an invalid geometry
                                                                        # "Ignore all invalid geometries and attempt to create the graph anyway"
                                                 

    else:  

        al_gdf = Input_Gdf
        graph = Graph.from_geodataframe(al_gdf, ignore_errors=True)

    #######################################################################################################################################################################################################
    # UPDATERS #
    #######################################################################################################################################################################################################

    # These are manually defined updaters. Some will be used as constraints, others are just to record information.

    # I made three different updates to track county splitting
    # All three pull information from the built-in updater.county_splits function, which I labeled "county_info" for a little more readability

    # "split_counter" records the number of splits that occur. 
    # If a county is split into two districts, that's one split, if a county is split into three districts, that's two splits
    def split_counter(partition):
        count_splits = 0
        for item in partition["county_info"]:
            count_splits += len(partition["county_info"][item][2]) - 1
        return count_splits


    # "num_split_counties" records the number of counties that have been split at least once. 
    # If a county is split into two districts, that's one split county, if a county is split into three districts, that's still one split county
    def num_split_counties(partition):
        counties_with_splits = 0
        for item in partition["county_info"]:
            if len(partition["county_info"][item][2]) > 1:
                counties_with_splits += 1
        return counties_with_splits

    # "oversplit_counties" tracks the number of counties that have been split more than once, as some states don't allow plans that do that.
    # If a county is split into two districts, that's zero oversplit counties, if a county is split into three or more districts, that's one oversplit county
    def oversplit_counties(partition):
        oversplits = 0
        for item in partition["county_info"]:
            if len(partition["county_info"][item][2]) > 2:
                oversplits += 1
        return oversplits


    # bad_incumbents tracks the number of districts in a plan with two or greater incumbents by tallying up the number of incumbents in all the VTDs for each district
    def bad_incumbents(partition):
        num_problems = 0
        for part in partition.parts:
            if partition["incumbents"][part] > 1:
                num_problems += 1
        return num_problems

    # subpop_over_threshold tracks the number of districts above a threshold value for a defined subpopulation. 
    def subpop_over_threshold(partition):
        count_over = 0
        alpha = 0.5 # Arbitrarily defined threshold
        for dist in partition['population']:
            # whatever this first term is needs to be an updater in my updaters so the constraint validator can look for it
            if partition['subpop'][dist] / partition['population'][dist] >= alpha:
                count_over += 1
        return count_over

    # Selecting/defining updaters that gerrychain should calculate and keep track of over each iteration
    # Gerry chain keeps track of several automatically (area, perimeter, cut_edges, etc.)

    my_updaters = {
        "population": updaters.Tally("total", alias="population"), # Population updater, for computing how close to equality the district populations are
        "county_info": updaters.county_splits("county_info", "county"),# built in function to generate county info.
        "split_counter": split_counter,# pulls from county_info to get total number of county splits.
        "num_split_counties": num_split_counties,
        "oversplit_counties": oversplit_counties,
        "incumbents": updaters.Tally("incumbent", alias="incumbents"),
        "bad_incumbents": bad_incumbents,
        "subpop": updaters.Tally("blk_1", alias="subpop"),
        "subpop_dists": subpop_over_threshold,
        }

    ############################################################################################################################################################################################################################
    # CONSTRAINTS AND CHAIN DETAILS #
    ############################################################################################################################################################################################################################

    ###########################################################################
    # This defines the initial partition that gets fed into the Markov Chain. #
    ###########################################################################

    # graph is the object it pulls info from, assignment= is the grouping variable for the starting district plan, and updaters= are your updaters (see above)

    initial_partition = GeographicPartition(graph, assignment="cd_117", updaters=my_updaters) # GeographicPartitian comes with built-in area and perimeter updaters for compactness.
                                                                                            # I'm guessing that cd_117 means the district map from the 117th congress (01/2021-01/2023).
                                                                                                # So the initial partitian is just the original map according to 117th congress. 
                                                                                                # al_gdf["cd_117"] assigns nodes to districts as a dictionary. See below.

    # The ReCom proposal needs to know the ideal population for the districts so that
    # we can improve speed by bailing early on unbalanced partitions.
    ideal_population = sum(initial_partition["population"].values()) / len(initial_partition)


    # We use functools.partial to bind the extra parameters (pop_col, pop_target, epsilon, node_repeats)
    # of the recom proposal.
    pop_deviation = 0.005

    # This is the syntax for the Recom algorithm. I don't know what node_repeats means, default was 10 so I kept it there.
    proposal = partial(recom,
                    pop_col="total",
                    pop_target=ideal_population,
                    epsilon=pop_deviation,
                    node_repeats=10
                    )

    # I manually define constraints below.

    # Keep populations with percent of ideal
    pop_constraint = constraints.within_percent_of_ideal_population(initial_partition, pop_deviation)


    # Prebuilt contiguity constraint
    contiguity = constraints.contiguous

    # # Prebuilt constraint that refuses partitions that created new county splits.
    # county_limit = constraints.refuse_new_splits("county_info")

    # Homebrewed alternative to "refuse_new_splits" that lets you define how many splits are allowed
    county_limit = constraints.UpperBound(
        lambda p: split_counter(p),
        3*split_counter(initial_partition)
    )

    # Setting parameters for chain. Proposal (recom vs flip vs other), constraints, any additional acceptance criteria, initial state, and total number of iterations
    chain = MarkovChain(
        proposal=proposal,
        constraints=[
            pop_constraint,
            contiguity,
            county_limit,
        ],
        accept=accept.always_accept,
        initial_state=initial_partition,
        total_steps = Total_Steps # IMPORTANT - this is the number of iterations you run. The first will be the initial plan. and then it will produce n-1 new plans for a total of n plans
    ) 
    
    print('0')
    return (graph, chain, al_gdf)


def RunGerryChain(GDF, Graph, Chain, VTD_Data='', Checkpoint = 1, Print_Iterations = False, Output_Csv = '', Output_Metadata = '', Output_Seg=''):
    """Returns (chain_list, metadata_list)"""

    ##########################################################################################################################################################################
    # RUNNNING CHAIN AND SAVING DATA #
    ##########################################################################################################################################################################

    # Initializing variables
    count = 0# recording iteration count
    chain_list = []# blank list to vtd assignment for each iteration
    metadata_list = []# blank list to store metadata
    tot_num_edges = Graph.number_of_edges() #will be used to calculate proportion of edges not cut
    start_time = time.time()

    Output_Csv = Output_Csv + '_0'
    Output_Metadata = Output_Metadata + '_0'
    Output_Seg = Output_Seg + '_0'

    # The Chain itself
    for partition in Chain:
        count += 1
        # bad_incumbents = 0
        part_dict = {"Iteration": count}
        meta_dict = {"Iteration": count}

        if Print_Iterations is True:

            print("Iteration " + str(count)) # uncomment line if you want to watch the progress

        # Storing all the metadata. I tried to find a better way to code this, but I ended up just hardcoding it in.
        # Dictionary key will be the column name in your metadata output, so you can make it however readable you want it to be
        meta_dict["incumbent_issues"] = partition.bad_incumbents
        meta_dict["prop_uncut_edges"] = 1 - len(partition["cut_edges"])/tot_num_edges
        meta_dict["county_splits"] = partition.split_counter
        meta_dict["num_split_counties"] = partition.num_split_counties
        meta_dict["oversplit_counties"] = partition.oversplit_counties
        meta_dict["black_majority"] = partition.subpop_dists


        # Extracting and storing assingment for each VTD
        for nodenum in range(len(partition.graph.nodes)):
            part_dict[str(partition.graph.nodes[nodenum]['vtd_cd_117'])] = partition.assignment[nodenum]
        chain_list.append(part_dict)
        metadata_list.append(meta_dict)

 
        if count % Checkpoint == 0:

            GerryChainOutput(Chain_List=chain_list, Metadata_List=metadata_list, Output_Csv=Output_Csv, Output_Metadata=Output_Metadata)
            SegregationOutput(VTD_Data=VTD_Data,Chain_List=chain_list,Output_Seg = Output_Seg)


            # Reset

            chain_list = []
            metadata_list = []

            Output_Csv = Output_Csv[:-2] + '_' + str(count)
            Output_Metadata = Output_Metadata[:-2] + '_' + str(count)
            Output_Seg = Output_Seg[:-2] + '_' + str(count)

    end_time = time.time()

    print("\nChain complete. Time elapsed: " + str(end_time-start_time) + " seconds.")

    print('0')
    return (chain_list, metadata_list, part_dict, seg_output)



def GerryChainOutput(Chain_List, Metadata_List, Output_Csv = '', Output_Metadata = ''):
    """Output_Csv = string path to the csv file output. This will create document with the given path & name
        Output_Metadata = string path to the metadata file output. This will create document with the given path & name
        Returns (transposed_csv, transosed_meta)"""

    ##########################################################################################################################################################################
    # OUTPUT ASSIGNMENT AND METADATA #
    ##########################################################################################################################################################################

    print("Outputing Data")
    
    # Path to your output for the vtd district assignments
    output_csv = Output_Csv

    # Path to the metadata output file
    output_metadata = Output_Metadata

    # turning list of dictionaries indo pandas dataframe object for easy manipulation
    chain_df = pd.DataFrame(Chain_List)

    # Transposing and saving dataframe as csv. 
    with open(output_csv, 'x', encoding='utf8', newline='') as output_file:
        transposed_csv = chain_df.T
        transposed_csv.to_csv(output_file, sep=',', encoding='utf-8', header=False)

    meta_df = pd.DataFrame(Metadata_List)

    # Transposing and saving dataframe as csv
    with open(output_metadata, 'x', encoding='utf8', newline='') as output_file:
        transposed_meta = meta_df.T
        transposed_meta.to_csv(output_file, sep=',', encoding='utf-8', header=False)


    print('0')
    return(transposed_csv, transposed_meta)

def CalculateSegregation(data_file_path='',vtds=[]):
    """Calculate the S+/- score for a selection of vtds
    data_file_path must include aggregated local environment data for the vtds
    vtds is a list of vtd id numbers"""

    import pandas as pd

    vtd_data = pd.read_csv(data_file_path)

    district_data = vtd_data[vtd_data['block_vtda'].isin(vtds)]

    total_population = district_data['total'].sum()
    total_weighted = district_data['weighted_blk_sl_dist'].sum()
    pct_black_district = district_data['nh_black'].sum() / total_population * 100 

    seg_score = pct_black_district - (total_weighted/total_population)

    return seg_score

def SegregationOutput(Chain_List=[],VTD_Data='',Output_Seg=''):

    # Clean District Assignments
    dist_assign = pd.DataFrame.from_dict(Chain_List)
    dist_assign = dist_assign.transpose()
    dist_assign = dist_assign.rename(columns=dist_assign.iloc[0]).drop(dist_assign.index[0])
    dist_assign.index = dist_assign.index.str[:-6]


    districts = list(dist_assign[dist_assign.columns[0]].unique())


    segregation_output = pd.DataFrame(index=districts)


    for i in list(dist_assign.columns):
        score_list = []
        for district in districts:
            dist_id = dist_assign[i] == district
            vtd_list = list(dist_assign[dist_id].index)
            seg_score = CalculateSegregation(data_file_path=VTD_Data,vtds=vtd_list)
            score_list.append(seg_score)
        segregation_output[i] = score_list

    with open(Output_Seg, 'x', encoding='utf8', newline='') as output_file:
        segregation_output.to_csv(output_file, sep=',', encoding='utf-8', header=True)

    return segregation_output
