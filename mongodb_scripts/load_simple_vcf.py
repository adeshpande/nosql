#!/usr/bin/python 

''' 
Simple Script to load a vcf to a mongo db database 
'''

import os
import sys
import csv 

## pyMongoDB dependencies 
from pymongo import MongoClient
from pymongo import Connection 
from pymongo.errors import ConnectionFailure

def main():
    '''Connects to MongoDB '''

    ## Connect to MongoDB 
    try:
        #client = MongoClient()
        conn = Connection()
        print "Connection Succesfull"    
    except ConnectionFailure, e:
        print "Couldnt connect to Default mongo client, %s" % e
        sys.exit(1)

    ## Get input file 
    filename = sys.argv[1]
    db_name  = os.path.basename(filename).split('.')[0] 

    ## Get a database handle 
    #db_handle = client[db_name]

    ## Assert database connection 
    #assert db_handle.connection == client 

    ## Create a database
    print "Creating database %s" % db_name
    vcf_db = conn[db_name]
    
    ## Create a collection (Table)
    vcf_collection = vcf_db['sites']

    ## Create array of Dictionaries 
    vcf_array = createDictfromVCF(filename)
    
    ## Insert Array into the database

    vcf_id = vcf_collection.insert(vcf_array)
    vcf_collection.find_one()



def createDictfromVCF(vcf_file):
    '''Creates a List of Dictionaries for each site from standard VCF file'''
    
    ## Open Vcf file 
    vcf_reader = csv.reader(open(vcf_file, 'r'), delimiter='\t')
   
    ## Define List 
    vcf_array = []

    ## Iterate 
    site_index = 0
    for line in vcf_reader:
        if line[0].startswith('##'):
            pass
        elif line[0].startswith('#'):
            line[0] = line[0].strip('#')
            header = line 
        else:
            ## define empty site dictionary 
            site_dict = {} 
            ## populate site dict
            field_index=0
            for field in line:
                site_dict[header[field_index]] = line[field_index] 
                field_index += 1
            ## append to final List
            vcf_array.append(site_dict)
            
        site_index += 1
    print vcf_array
    print "Done %s sites " % site_index
        
    return vcf_array
    
        

if __name__=="__main__":
    main()
