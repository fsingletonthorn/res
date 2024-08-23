def extract_features(listing):
    features = listing.findAll('span', attrs={"data-testid": "property-features-text-container"})
    feature_dict = dict()
    # Bedrooms
    for j in range(len(features)):
        feature = features[j].find('span', attrs={"data-testid": "property-features-text"})
        if feature is None:
            feature = 'unknown'
        else:
            feature = feature.text
        value =  features[j].text.split(' ')[0]
        feature_dict.update({feature: value})

    return(feature_dict)
