import os 
import numpy as np
from glob import glob 
import geopandas as gpd
from datetime import datetime as dt

from gerrychain import Graph, GeographicPartition


class Chain:

    def __init__(self, steps:int, pop_deviation:int):

        self.steps = steps
        self.pop_deviation = pop_deviation

        self.updaters = {}
        self.constraints = []

        self.gdf = None
        self.current_partition = None

    def create_initial_partition(self, shp_file:str, grouping_assignment_column:str):

        self.gdf = gpd.read_file(shp_file)
        graph = Graph.from_geodataframe(self.gdf, ignore_errors=True) 

        self.current_partition = GeographicPartition(graph, assignment=grouping_assignment_column, updaters=self.get_updaters())

    def update_partition(self, new_gdf:gpd.GeoDataFrame, grouping_assignment_column:str):

        graph = Graph.from_geodataframe(new_gdf, ignore_errors=True) 

        self.current_partition  = GeographicPartition(graph, assignment=grouping_assignment_column, updaters=self.get_updaters())
    
    def get_current_partition(self):

        return self.current_partition

    def define_updater(self, updater_name:str, updater_function):

        self.updaters.update({updater_name:updater_function})

    def remove_updater(self, updater_name:str):
        
        self.updaters.pop(updater_name)

    def get_updaters(self):
        '''this function should return a dictionary of updaters to pass into RunGerryChain'''

        return self.updaters

    def define_constraint(self, constraint):

        try:
            self.constraints.append(constraint)
        except Exception as err:
            print("error: " + err)

    def remove_constraint(self, constraint):

        try: 
            self.constraints.remove(constraint)
        except ValueError as err:
            print("error: " + err)

    