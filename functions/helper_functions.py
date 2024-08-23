import pandas as pd

def clean_listings(residential_listings_search_output):
    """
    A function to take residential_listings_search() output and convert it into a flat table.
    
    Parameters
    ----------
    residential_listings_search_output: obj
        The results of residential_listings_search(...) to b cleaned
    """
    # Now dealing with the nested data
    listings = [value for value in residential_listings_search_output['listings'].values()]

    listings_json = pd.json_normalize(listings)

    projects = listings_json.loc[listings_json['type'] == 'Project']

    property_listings = listings_json.loc[listings_json['type'] == 'PropertyListing']

    if len(projects) > 0:
        ## Need to deal with projects that have nested json even after normalization - pd.json_normalize(test.loc[test['type'] == 'Project']['listings'][0])
        project_listings = pd.concat(list(map(pd.json_normalize, projects['listings']))).add_prefix('listing.')

        ## Getting all project data that are not dupes
        project_metadata = projects[projects.columns[~projects.columns.isin(project_listings.columns)]]
        
        project_metadata = project_metadata.drop_duplicates(subset='project.id')

        project_listings_w_meta = project_metadata.merge(project_listings, left_on='project.id', right_on='listing.projectId', how = 'left', validate = 'one_to_many')

        ## Dropping dupe col
        project_listings_w_meta = project_listings_w_meta.drop('listing.projectId', axis = 1)

        # Concatinating all listings together, dropping nested listings col (now joined on)
        all_listings = pd.concat([project_listings_w_meta, property_listings], ignore_index = True).drop('listings', axis = 1)

    elif any(property_listings.columns == 'listings'):
        all_listings = property_listings.drop('listings', axis = 1)
    else: 
        all_listings = property_listings

    all_listings.columns = (all_listings.columns
                    .str.replace('.', '_', regex=False)
                )
    
    return(all_listings)