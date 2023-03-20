# no thread
import geopandas
import numpy as np
import threading
import time

class ConfigValidator:
    def __init__(self, config, shapefile):
        self.config = config
        self.shapefile = shapefile
        self.gdf = geopandas.read_file(shapefile)
        self.st = time.time()
        for i in range(12):
            self.gdf = self.gdf.append(self.gdf)
        # for i in range(4):
        #     self.gdf = self.gdf.append(self.gdf)
        # self.gdf = self.gdf.append(self.gdf)
        self.now = time.time()        
        print(len(self.gdf), self.now - self.st)
        self.NUM_THREADS = 1

    def validate_config_structure(self):
        config = self.config
        if 'attributes' not in config: 
            raise ValueError('Invalid Config - Property "attributes" not found')
        if 'geometry' not in config:
            raise ValueError('Invalid Config - Property "geometry" not found')
        if 'dtypes' in config['attributes']:
            valid_types = ['int', 'int64', 'float', 'double', 'text', 'objectID', 'date']
            for key in config['attributes']['dtypes'].keys():
                if key not in valid_types:
                    raise ValueError(f'Invalid Config - invalid dtype - "{key}"')
        # we can have more validations on this, like valid functions, valid featurename lists etc 
        # not needed as config will be generated from template or stored

    def parallel_execution(self, function, *args):
        threads = []
        for i in range(self.NUM_THREADS):
            thread = threading.Thread(target=function, args=(i,) + args)
            threads.append(thread)

        # start the threads
        for thread in threads:
            thread.start()

        # wait for all threads to finish
        try:
            for thread in threads:
                thread.join()
        except:
            for thread in threads:
                thread._stop()
             
    def dtypes_validation(self):
        config = self.config
        if 'dtypes' in config['attributes']:
            # gdf = self.gdf
            # read shapefile in geopandas - dtype in pandas - object, int64, float64, datetime64, bool
            shapefile_dtypes = self.gdf.dtypes
            # mp to map standard types to pandas types
            mp = {
                'int' : np.dtype('int'),
                'int64' : np.dtype('int64'),
                'float' : np.dtype('float'),
                'float64' : np.dtype('float64'),
                'double' : np.dtype('float'),
                'text' : np.dtype('object_'),
                'objectID' : np.dtype('object_'),
                'date' : np.dtype('datetime64')
            }
            for dtype in config['attributes']['dtypes']:
                for featurename in config['attributes']['dtypes'][dtype]:
                    if(shapefile_dtypes[featurename] != mp[dtype]):
                        raise ValueError(f'Invalid data type for {featurename}, should be {mp[dtype]} but is {shapefile_dtypes[featurename]}')

    def inclusive_range_validation(self, thread_id, featurename):
        # lower <= all_vals <= upper 
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        bounds = self.config['attributes']['ranges']['inclusive'][featurename]
        lower, upper = bounds[0], bounds[1]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & ((self.gdf[featurename] < lower) | (self.gdf[featurename] > upper))]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value outside [{lower}, {upper}] found')

    def exclusive_range_validation(self, thread_id, featurename):
        # all_vals < lower or all_vals > upper 
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        bounds = self.config['attributes']['ranges']['exclusive'][featurename]
        lower, upper = bounds[0], bounds[1]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & (self.gdf[featurename] >= lower) & (self.gdf[featurename] <= upper)]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value found in range [{lower}, {upper}]')

    def ranges_validation(self):
        config = self.config
        if 'ranges' in config['attributes']:
            if 'inclusive' in config['attributes']['ranges']:
                # for all featurename in inclusive, we parallelly execute validator function 
                for featurename in config['attributes']['ranges']['inclusive'].keys():
                    self.parallel_execution(self.inclusive_range_validation, featurename)

            if 'exclusive' in config['attributes']['ranges']:
                # for all featurename in exclusive, we parallelly execute validator function 
                for featurename in config['attributes']['ranges']['exclusive'].keys():
                    self.parallel_execution(self.exclusive_range_validation, featurename)

    def equal_value_validation(self, thread_id, featurename):
        # all_vals = val
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        val = self.config['attributes']['values']['equal'][featurename]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & (self.gdf[featurename] != val)]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value found not equal to {val}')

    def not_equal_value_validation(self, thread_id, featurename):
        # all_vals != val
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        val = self.config['attributes']['values']['not_equal'][featurename]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & (self.gdf[featurename] == val)]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value found equal to {val}')

    def values_validation(self):
        config = self.config
        if 'values' in config['attributes']:
            if 'equal' in config['attributes']['values']:
                for featurename in config['attributes']['values']['equal'].keys():
                    self.parallel_execution(self.equal_value_validation, featurename)

            if 'not_equal' in config['attributes']['values']:
                for featurename in config['attributes']['values']['not_equal'].keys():
                    self.parallel_execution(self.not_equal_value_validation, featurename)
    
    def inclusive_subset_validation(self, thread_id, featurename):
        # all_vals belongs to vals
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        vals = self.config['attributes']['subsets']['inclusive'][featurename]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & (~self.gdf[featurename].isin(vals) & (self.gdf[featurename] != None))]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value found which does not belong to the {vals}')
        
    def exclusive_subset_validation(self, thread_id, featurename):
        # all_vals not belongs to vals
        # gdf = self.gdf
        batch_size = len(self.gdf)//self.NUM_THREADS
        # thread_id represents l to r in gdf
        l = thread_id * batch_size
        r = min(len(self.gdf), l + batch_size) - 1
        if(thread_id == self.NUM_THREADS - 1):
            r = len(self.gdf) - 1
        vals = self.config['attributes']['subsets']['exclusive'][featurename]
        filtered_gdf = self.gdf[(self.gdf.index >= l) & (self.gdf.index <= r) & (self.gdf[featurename].notnull()) & (self.gdf[featurename].isin(vals))]
        if(len(filtered_gdf) > 0):
            raise ValueError(f'Invalid value for {featurename}, value found which belongs to the {vals}')

    def subsets_validation(self):
        config = self.config
        if 'subsets' in config['attributes']:
            if 'inclusive' in config['attributes']['subsets']:
                for featurename in config['attributes']['subsets']['inclusive'].keys():
                    self.parallel_execution(self.inclusive_subset_validation, featurename)

            if 'exclusive' in config['attributes']['subsets']:
                for featurename in config['attributes']['subsets']['exclusive'].keys():
                    self.parallel_execution(self.exclusive_subset_validation, featurename)
        
    def not_null_validation(self):
        config = self.config
        if 'not_null' in config['attributes']:
            for featurename in config['attributes']['not_null']:
                if self.gdf[featurename].isnull().any():
                    raise ValueError(f'Invalid value for {featurename}, null value found')

    def attributes_check_functions_validation(self):
        config = self.config
        if 'check_functions' in config['attributes']:
            pass

    def crs_validation(self):
        config = self.config
        if 'crs' in config['geometry']:
            if(str(self.gdf.crs) != config['geometry']['crs']):
                raise ValueError(f'Invalid crs {str(self.gdf.crs)} found')
            
    def geometry_types_validation(self):
        config = self.config
        # workaround for now...
        if 'types' in config['geometry']:
            valid_types = config['geometry']['types']
            types_found = set(self.gdf.geom_type)
            for type in types_found:
                if type not in valid_types:
                    raise ValueError(f'Invalid geometry type {type} found, it should be from {valid_types}')
            
    def geometry_check_function_validation(self):
        config = self.config
        if 'check_functions' in config['geometry']:
            pass

    def validate(self):
        self.validate_config_structure()
        #### ATTRIBUTES ####
        # dtypes validation,
        self.dtypes_validation()
        # ranges validation,
        self.ranges_validation()
        # values validation,
        self.values_validation()
        # subsets validation, (considered for only belonging condition)
        self.subsets_validation()
        # not_null validation,
        self.not_null_validation()
        # check_functions validation, (run function for all values in feature, all must be true)
        self.attributes_check_functions_validation()
        
        #### GEOMETRY VALIDATION ####
        # crs validation,
        self.crs_validation()
        # types validation
        self.geometry_types_validation()
        # check_functions validation
        self.geometry_check_function_validation()
        print('end', time.time() - self.now)

