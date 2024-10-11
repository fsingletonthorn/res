import pandas as pd

def clean_listings(residential_listings_search_output, debug = False):
    """
    A function to take residential_listings_search() output and convert it into a flat table.
    
    Parameters
    ----------
    residential_listings_search_output: obj
        The results of residential_listings_search(...) to b cleaned
    """
    if debug:
        print('Total listings: '+ str(len(residential_listings_search_output['listings'])))
        print('Total unique listings by string: ' + str(len(set(residential_listings_search_output['listings'].keys()))))

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

        if debug:
            print('Debug - Project listing ids duplicated: ' + str(project_listings_w_meta['listing.id'].duplicated().sum()))

        # Concatinating all listings together, dropping nested listings col (now joined on)
        all_listings = pd.concat([project_listings_w_meta, property_listings], ignore_index = True).drop('listings', axis = 1)

    elif any(property_listings.columns == 'listings'):
        all_listings = property_listings.drop('listings', axis = 1)
    else: 
        all_listings = property_listings
    
    if debug:
        print('Debug - all listing ids duplicated: ' + str(all_listings['listing.id'].duplicated().sum()))

    all_listings.columns = (all_listings.columns
                    .str.replace('.', '_', regex=False)
                )
    
    return(all_listings)