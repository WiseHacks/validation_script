# General validation config for a shapefile 
config = {
    'attributes' : {
        'dtypes' : {
            'int' : ['featurename'],
            'int64' : [],
            'float' : [],
            'float64' : [],
            'double' : [],
            'text' : [],
            'objectID' : [],
            'date' : [],
            'json' : []
        },
        'ranges' : {
            # all values
            'inclusive' : {
                'featurename' : ['lower', 'uppper'], #lower <= val <= upper
                # ...
                # can have mulitple ranges
            },
            'exclusive' : {
                'featurename' : ['lower', 'upper'], #val < lower, val > upper
                # ...
                # can have multiple ranges
            }
        },
        'values' : {
            'equal' : {
                'featurename' : 'val', 
                # ...
            },
            'not_equal' : {
                'featurename' : 'val', 
                # ...
            }
        },
        'subsets' : {
            # all values
            'inclusive' : {
                'featurename' : ['values'],
                # ...
            },
            'exclusive' : {
                'featurename' : ['values'],
                # ...
            }
        },
        'not_null' : ['features which must not have null, missing values'],
        # 'null' : ['features which can be null'],
        # custom functions for complex checks like, for town = oldtown, all featurestatus should be active, something like that
        'check_functions' : {
            'featurename' : ['function definitions in python maybe']
        }
    },
    'geometry' : {
        # there will be only one column for geometry, which can be of type point, polygon, etc
        'crs' : '',
        'types' : ['Point'],
        'check_functions' : []
        # it depends, maybe area, distance, intersection pairwise, number of overlaps, it can be anything
        # which is passed as functions, so will it be a good idea? 
    }
}