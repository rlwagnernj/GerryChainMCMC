import numpy as np
import matplotlib.pyplot as plt
import matplotlib.artist as artist
import matplotlib.colors as mcolors
import geopandas as gpd

shp_file = '/Users/rebeccawagner/Documents/GitHub/GerryGainMCMC/Data/Alabama shp files/Alabama_VTD_District_Intersection.shp'
gdf = gpd.read_file(shp_file)
gdf["incumbent"] = gdf["incumbent"].fillna(0)

gdf['centroid'] = gdf['geometry'].centroid
gdf["centroid_x"] = gdf['geometry'].centroid.x
gdf["centroid_y"] = gdf['geometry'].centroid.y

fig, ax = plt.subplots(1,1,figsize=(20,20))
gdf.plot(ax=ax, picker=True)
ax.scatter(gdf['centroid_x'], gdf['centroid_y'], s=2, color='k', alpha=.5, picker=True)

ax.axis('off')
#plt.show()

patch_collection = ax.collections[0]
path_collection = ax.collections[1]

n = len(patch_collection.get_paths())
facecolors = patch_collection.get_facecolors()
#if len(facecolors) == 1 and n != 1:
facecolors = np.array([facecolors[0]] * n)

def on_pick(event):

    ind = event.ind
    print(ind)
    facecolors[ind] = mcolors.to_rgba("crimson")
    patch_collection.set_facecolor(facecolors)

    fig.canvas.draw()
    

cid = fig.canvas.mpl_connect('pick_event', on_pick)

plt.show()
