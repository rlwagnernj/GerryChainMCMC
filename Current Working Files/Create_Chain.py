from Chain_Class import Chain

from gerrychain import updaters, constraints
from gerrychain.updaters import Tally, cut_edges

##################################################

def CreateChain(steps:int, shp_file:str, grouping_assignment_column:str, updaters_list:list, pop_deviation=.05):

    print("creating chain object..")
    chain = Chain(steps=steps,pop_deviation=pop_deviation) # create instance of chain class

    print('defining updaters...')
    # define updaters 
    for updater in updaters_list:
        chain.define_updater(updater_name=updater.__name__,updater_function=updater)

    # haven't come up with a better way to have the user include these. They're built in for now, dependant on the data header
    chain.define_updater(updater_name="population", updater_function=updaters.Tally("total",alias="population"))
    chain.define_updater(updater_name='county_info',updater_function=updaters.county_splits("county_info","county"))
    chain.define_updater("incumbents",updaters.Tally("incumbent",alias='incumbents'))
    chain.define_updater("subpop",updaters.Tally("blk_1",alias='subpop'))

    print('creating partition')
    # create initial partition
    chain.create_initial_partition(shp_file=shp_file, grouping_assignment_column=grouping_assignment_column)
    
    print('chain object with no constraints created')
    return chain

##################################################

def AddConstraints(chain:Chain, constraints_list:list):
    for constraint in constraints_list:
        chain.define_constraint(constraint)
    
    return 0