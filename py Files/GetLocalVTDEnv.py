def GetVTDLocalEnv(Block_LE_Data,Block_to_Vtd,save=False,save_path=''):
    """Get the local environment data necessary for calculating S+/- scores on VTDs
        block_LE_data = string path to csv file local environment blocks for the state
        block_to_vtd = string path to csv file mapping blocks to vtds
        set save = True to save as cvs file to local device
        set save path as string"""
    
    # import libraries
    import pandas as pd

    # get block to VTD mapping
    block_to_vtd = pd.read_csv(Block_to_Vtd, dtype={'GEOID20': str})[["GEOID20","block_vtda"]] # keep only necessary columns
    #block_to_vtd["GEOID20"] = block_to_vtd["GEOID20"].astype(str) # cast type for merging 

    # local environment data for blocks
    header = pd.read_csv(Block_LE_Data, index_col=0, nrows=0).columns.tolist()
    block_le_data = pd.read_csv(Block_LE_Data, dtype={'block_geoid_start':str}).reset_index(drop=True) 

    # merge dataframes
    merged_df = block_to_vtd.merge(right=block_le_data, left_on='GEOID20', right_on='block_geoid_start').drop(columns=["block_geoid_start"])
    #merged_df[["GEOID20","block_vtda","rn_total_sl_dist","rn_nh_black_sl_dist","total","nh_black"]]

    # make new columns for S+/- calculations
    merged_df["pct_blk_sl_dist"] = merged_df["rn_nh_black_sl_dist"]/merged_df["rn_total_sl_dist"] * 100
    merged_df["weighted_blk_sl_dist"] = merged_df["pct_blk_sl_dist"] * merged_df["total"]

    # Sum columns for each VTD
    final_df = merged_df.groupby(by="block_vtda",as_index=False).sum()[["block_vtda","total","nh_black","weighted_blk_sl_dist"]]

    # save as csv if required
    if save is True:
        final_df.to_csv(path_or_buf=save_path)

    # return 
    return final_df
    