import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import tkinter as tk
import matplotlib.colors as mcolors

shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/VA?/tl_2020_51_vtd20.shp'
gdf = gpd.read_file(shp_file)

gdf['centroid'] = gdf['geometry'].centroid
gdf["centroid_x"] = gdf['geometry'].centroid.x
gdf["centroid_y"] = gdf['geometry'].centroid.y

gdf = gdf[["geometry","centroid_x","centroid_y"]]
#gdf = gdf[:10]


def select(ind):

    if ind in [3,151,278,1405,2000]:
        facecolors[ind] = mcolors.to_rgba("crimson")
        facecolors[ind + 1] = mcolors.to_rgba("crimson")
        patch_collection.set_facecolor(facecolors)

    else:
        print(facecolors[ind])
        facecolors[ind] = mcolors.to_rgba("crimson")
        patch_collection.set_facecolor(facecolors)
        print(facecolors[ind]) 

    fig.canvas.draw()

def deselect(ind):

    if ind in [3,151,278,1405,2000]:
        facecolors[ind] = mcolors.to_rgba("blue")
        facecolors[ind + 1] = mcolors.to_rgba("blue")
        patch_collection.set_facecolor(facecolors)

    else:
        print(facecolors[ind])
        facecolors[ind] = mcolors.to_rgba("blue")
        patch_collection.set_facecolor(facecolors)
        print(facecolors[ind]) 

    fig.canvas.draw()

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

    #print(ind_1)
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
    #print(ind_2)

    if (facecolors[ind_2]==[[0.12156863, 0.46666667, 0.70588235, 1.        ]]).all: 
        print("color is blue")
        select(ind_2)

    elif (facecolors[ind_2]==[[0.8627451,  0.07843137, 0.23529412, 1.        ]]).all:
        print("color is red")
        deselect(ind_2)



cid = fig.canvas.mpl_connect('pick_event', onpick)

plt.show()

