######################################################################
# Set Up #
######################################################################

## import libraries ##
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

## get files - you'll need to change these paths for your own computer ##
shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/State shp files /VA/tl_2020_51_vtd20.shp'
data_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/VA_data.csv'

## open data file ##
data = pd.read_csv(data_file, dtype= {"block_vtda": str})

## set up gdf ##
gdf = gpd.read_file(shp_file)

gdf['centroid'] = gdf['geometry'].centroid ## centroid for picking ##
gdf["centroid_x"] = gdf['geometry'].centroid.x
gdf["centroid_y"] = gdf['geometry'].centroid.y

gdf = gdf[["GEOID20","geometry","centroid_x","centroid_y"]] ## keep only needed columns ##

## initialize selected df ##
## this df is for keeping track of which VTD's are selected and which aren't ##
## 0 means not selected, 1 means it is ##
selected_df = pd.DataFrame() 
selected_df["vtd"] = gdf["GEOID20"]
selected_df["check"] = 0

## initialize score list ##
## list of VTDs to be used in calculating a seg. score ##
score_list = []

## initialize population variables ##
population = 0
target_pop = (data["total"].sum()/11) ## total VA population divided by number of districts ##
target_pop_min = target_pop - target_pop * .1 ## 10% range around target pop ##
target_pop_max = target_pop + target_pop * .1
population_range = [target_pop_min, target_pop, target_pop_max]

########################################################################
# Functions #
########################################################################

def CheckPopulation(population, population_range):

    if population < population_range[2]:
        return 0

    else:
        return 1

def CalculateSegregation(vtds=[]):
    """Calculate the S+/- score for a selection of vtds
    data_file_path must include aggregated local environment data for the vtds
    vtds is a list of vtd id numbers"""

    district_data = data[data['block_vtda'].isin(vtds)]

    total_population = district_data['total'].sum()
    total_weighted = district_data['weighted_blk_sl_dist'].sum()
    pct_black_district = district_data['nh_black'].sum() / total_population * 100 

    seg_score = pct_black_district - (total_weighted/total_population)

    print("The S+/- Score is ", seg_score)

def AddVTD(vtd, ind, population, population_range):
    
    ## Check the population first ##
    ## get population of VTD ##
    vtd_pop = data[data["block_vtda"] == vtd].iloc[0]["total"]
    # increase population
    population += vtd_pop

    if CheckPopulation(population, population_range) == 0:
        print("The population of your district is ", population)
    else: 
        print("Your population is too large. The target population is in the range ",
                population_range[0], " - ", population_range[2])
        return 1 ## if the population would be too large, do not update the district ##

    ## update selected_df ##
    selected_df.loc[selected_df["vtd"] == vtd, "check"] = 1

    ## add to score list ##
    score_list.append(vtd)

    ## update patch color ##
    ## this if/else controls for the VTDs with multiple patches##
    if ind in [3,151,278,1405,2000]:
        facecolors[ind] = mcolors.to_rgba("crimson")
        facecolors[ind + 1] = mcolors.to_rgba("crimson")
        patch_collection.set_facecolor(facecolors)

    else:
        facecolors[ind] = mcolors.to_rgba("crimson")
        patch_collection.set_facecolor(facecolors)

    ## update figure ##
    fig.canvas.draw()

    CalculateSegregation(vtds=score_list)

def RemoveVTD(vtd, ind, population):

    ## get population of VTD ##
    vtd_pop = data[data["block_vtda"] == vtd].iloc[0]["total"]
    # decrease population
    population -= vtd_pop

    ## update selected_df ##
    selected_df.loc[selected_df["vtd"] == vtd, "check"] = 0

    ## remove from score list ##
    score_list.remove(vtd)

    ## update patch color ##
    ## this if/else controls for the VTDs with multiple patches##
    if ind in [3,151,278,1405,2000]:
        facecolors[ind] = mcolors.to_rgba("steelblue")
        facecolors[ind + 1] = mcolors.to_rgba("steelblue")
        patch_collection.set_facecolor(facecolors)

    else:
        facecolors[ind] = mcolors.to_rgba("steelblue")
        patch_collection.set_facecolor(facecolors)

    ## update figure ##
    fig.canvas.draw()

    CalculateSegregation(vtds=score_list)

## this function controls what happens when a vtd is clicked ##
## ind_1 is the index of the vtd in the data file ##
## ind_2 corresponds to the index of the vtd in the patch collection ##
## ind_1 != ind_2 because some VTDs have multiple patches associated with them ##
def click(ind_1, ind_2, population, population_range):

    ## get the VTD ID ##
    vtd = gdf.iloc[ind_1].iloc[0]["GEOID20"]

    ## check if the vtd is already selected ##
    select_value = selected_df[selected_df["vtd"] == vtd].iloc[0]["check"]

    if select_value == 0: ## The VTD is not selected yet ##
        AddVTD(vtd, ind_2, population, population_range)

    else: ## The VTD is already selected ##
        RemoveVTD(vtd, ind_2, population)


fig, ax = plt.subplots(1,1,figsize=(10,10))
gdf.plot(ax=ax)
ax.scatter(gdf['centroid_x'], gdf['centroid_y'], s=2, color='k', alpha=.5, picker=True)

ax.axis('off')

patch_collection = ax.collections[0]
n = len(patch_collection.get_paths())
facecolors = patch_collection.get_facecolors()
facecolors = np.array([facecolors[0]] * n)

def onpick(event):

    ind_1 = event.ind

    if (4 <= ind_1 < 152):
        ind_2 = ind_1 + 1
    elif (152 <= ind_1 < 279):
        ind_2 = ind_1 + 2
    elif (279 <= ind_1 < 1406):
        ind_2 = ind_1 + 3
    elif (1406 <= ind_1 < 2001):
        ind_2 = ind_1 + 4
    elif (2001 <= ind_1):
        ind_2 = ind_1 + 5
    else:
        ind_2 = ind_1

    click(ind_1, ind_2, population, population_range)

cid = fig.canvas.mpl_connect('pick_event', onpick)

plt.show()

