from Chain_Class import Chain  
from datetime import datetime as dt

############################################################

def time_stamp():
    return str(dt.now()).replace(' ', '-').replace(':', '-').replace('.', '-')

def seed_hist(cur_seed, seed_filename):

    if seed_filename is not None:
        # Get the random seed history from the filename
        seed_hist = seed_filename.split('_')[3]
        # Append the current seed to the seed history
        seed_hist = seed_hist + '.' + str(cur_seed)
    else:
        seed_hist = str(cur_seed)

    return seed_hist

############################################################

def RunGerryChain(chain_obj:Chain, seed:int, geographic_area_column:str, population_column:str, output_path:str, show_progress=False):

    #######################
    # Set seed if desired #
    #######################

    if seed:
        from gerrychain.random import random
        random.seed(seed)

    ##################
    # IMPORT MODULES #
    ##################

    from codecs import ignore_errors
    from mimetypes import init
    from re import sub
    from gerrychain import (GeographicPartition, Partition, Graph, MarkovChain,
                            proposals, updaters, constraints, accept, Election)
    from gerrychain.updaters import Tally, cut_edges
    import pandas as pd
    import os.path
    import json
    from gerrychain.proposals import recom
    from functools import partial
    import numpy as np
    import time

    # Additional modules for importing shapefile stuff
    import geopandas as gpd
    from scipy.stats import beta

    #################################
    # CONSTRAINTS AND CHAIN DETAILS #
    #################################

    partition = chain_obj.get_current_partition()

    # The ReCom proposal needs to know the ideal population for the districts so that
    # we can improve speed by bailing early on unbalanced partitions.
    ideal_population = sum(partition["population"].values()) / len(partition)

    # We use functools.partial to bind the extra parameters (pop_col, pop_target, epsilon, node_repeats)
    # of the recom proposal.
    pop_deviation = chain_obj.pop_deviation

    # This is the syntax for the Recom algorithm. I don't know what node_repeats means, default was 10 so I kept it there.
    proposal = partial(recom,
                    pop_col=population_column,
                    pop_target=ideal_population,
                    epsilon=pop_deviation,
                    node_repeats=10
                    )
    
    # Setting parameters for chain. Proposal (recom vs flip vs other), constraints, any additional acceptance criteria, initial state, and total number of iterations
    run_chain = MarkovChain(
        proposal=proposal,
        constraints= chain_obj.constraints,
        accept=accept.always_accept,
        initial_state=partition, 
        total_steps=chain_obj.steps 
    )

    ##################################
    # RUNNNING CHAIN AND SAVING DATA #
    ##################################

    # Initializing variables
    count = 0# recording iteration count
    chain_list = []# blank list to vtd assignment for each iteration
    metadata_list = []# blank list to store metadata
    tot_num_edges = partition.graph.number_of_edges() #will be used to calculate proportion of edges not cut
    start_time = time.time()

    # The Chain itself
    for partition in run_chain:
        count += 1
        # bad_incumbents = 0
        part_dict = {"Iteration": count}
        meta_dict = {"Iteration": count}
        if show_progress is True:
            print("Iteration " + str(count)) 

        my_updaters = chain_obj.get_updaters()
        for i in range(len(my_updaters)):
            meta_dict[list(my_updaters.keys())[i]] = partition[list(my_updaters.keys())[i]]

        # Extracting and storing assingment for each VTD
        for nodenum in range(len(partition.graph.nodes)):
            part_dict[str(partition.graph.nodes[nodenum][geographic_area_column])] = partition.assignment[nodenum]
        chain_list.append(part_dict)
        metadata_list.append(meta_dict)

    ## Chain output

    chain_df = pd.DataFrame(chain_list)
    with open(output_path, 'x', encoding='utf8', newline='') as output_file:
        transposed_csv = chain_df.T
        transposed_csv.to_csv(output_file, sep=',', encoding='utf-8', header=False)

    meta_df = pd.DataFrame(metadata_list)
    with open(output_path+'meta', 'x', encoding='utf8', newline='') as output_file:
        transposed_meta = meta_df.T
        transposed_meta.to_csv(output_file, sep=',', encoding='utf-8', header=False)
        
    end_time = time.time()

    print("\nChain complete. Time elapsed: " + str(end_time-start_time) + " seconds.")