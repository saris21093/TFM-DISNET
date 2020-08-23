"""Indication.

This module  works in order to get indication information and insert them into DISNET drugslayer database.

It fills 3 tables: phenotype_effect, code and has_code.

From the chEMBL python client the data for phenotype_effect are extracted.

The data for code and has_code are extracted with an API from UMLS Terminology Services REST API."""

import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor

# Get the de MeSH code and the UMLS code from the Previous Version of has_code table, and create a dictionary with MeSH as key and Code as  value
PV_has_code_table=get_list("select id,code from has_code where resource_id = 121")

# Get Previous Version  of code table
PV_code_table=get_list("select code from code where resource_id = 121")
PV_code_table=list(*zip(*PV_code_table))

# Get the primary keys (pk) from drug_phenotype_effect table Previous Version
# pe --> phenotype effect
PV_PK_pe_table = get_list("select phenotype_id from phenotype_effect")
PV_PK_pe_table=list(*zip(*PV_PK_pe_table))

# Get the Previous Version from drug_phenotype_effect table 
# pe --> phenotype effect
PV_pe_table = get_list("select * from phenotype_effect")


# Cross reference dictionary
mesh_umls_dic={}
for i in PV_has_code_table:
    mesh_code=i[0]
    umls_code=i[1]
    mesh_umls_dic[mesh_code]=umls_code

# Initialize auxiliaries

# Count for keeping the quantity of data that are going to be inserted
count=0

# These Lists are going to keep tuples with the NEW complete data which will be inserted in the tables
NEW_complete_indication_list=[]
NEW_complete_code_list=[]
NEW_complete_has_code_list=[]

# This List is going to keep the NEW pk of the data that will be inserted
NEW_indication_list=[]

# This variable will keep the intersection between old data and new data
intersection_indication = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_indication = 0
n_ins_code = 0
n_upd_indication = 0
n_del_indication = 0
n_same_indication = 0

# Source from data in this file were extracted
SOURCE = "CHEMBL"
# Get the source_id of SOURCE from the table "source"
source_id=int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])

# id_resoruce_id ---> MESH
ID_RESOURCE_ID = 75

# Resource id
UMLS_RESOURCE_ID = 121

# Entity id
DISEASE_ENTITY_ID = 1

# Get the info from chEMBL Python Client
from chembl_webresource_client.new_client import new_client
indications = new_client.drug_indication
source_cr = "MSH"


for i in indications:
    
    indication_mesh=i['mesh_id']
    indication_name_prev=i['mesh_heading']
    indication_name = indication_name_prev.replace("'", "\\'") # Avoid the problem with the "'" at the moment to insert the data to the Database we replace it by "\' "
   
    # INSERT: If the indication_mesh is already in the code and has_code table,
    # get the umls code from the dictionary created before
    # primary key (pk) that is not in the previous version
    # Sometimes there are repeat pk in the new data, 
    # There are several drug-phenotype combination with different max_phase, and
    # only the higher one will be taken.
    # in order to avoid duplicate primary key of the tables and choose the higher max_phase:
    # it keeps all the pk in a list.
    # Other list keeps tuples with the data which will be inserted in the tables.
    # If the pk is already in the pk list, it takes the index where the pk is in the list.
    # If the new frecuency is higher, the row in the index will be delete from the list with the tuple with all the information
    # and the list with the pk.
    # This method is faster than handle the exception and insert data one per one. 

    # UPDATE: primary key that is in the previous version of the table and in the new one
    # If all the data is the same it is repeat data
    # If the data is different is an update
    
   
    if not indication_mesh in mesh_umls_dic.keys():
        indication_umls=get_umls.get_umls(indication_mesh,indication_name_prev,source_cr)
        if indication_umls != None:
            mesh_umls_dic[indication_mesh]=indication_umls
            

    if not mesh_umls_dic[indication_mesh] in PV_code_table: 
        indication_umls=mesh_umls_dic[indication_mesh]
        code=(indication_umls,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
        has_code=(ID_RESOURCE_ID,indication_mesh,indication_umls,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
        NEW_complete_code_list.append(code)
        NEW_complete_has_code_list.append(has_code)
               
    if mesh_umls_dic[indication_mesh] in mesh_umls_dic.values():
        indication_umls=mesh_umls_dic[indication_mesh]
        indication=(indication_umls,indication_name)
        if not indication_umls in PV_PK_pe_table:
            if not indication_umls in NEW_indication_list:
                NEW_indication_list.append(indication_umls)
                NEW_complete_indication_list.append(indication)
                count+=1
                n_ins_indication +=1
        else:
            n_same_indication
          
                            

        # Insert the Data each 50 rows in each list
        if count == 50:
            cursor.executemany("insert into phenotype_effect values(%s,%s)",NEW_complete_indication_list)
            NEW_complete_indication_list = []
            count=0
                
# Insert the remaining data in the lists
cursor.executemany("insert into phenotype_effect values(%s,%s)",NEW_complete_indication_list) 
# Insert the UMLS code          
cursor.executemany("insert into code values(%s,%s,%s)",NEW_complete_code_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",NEW_complete_has_code_list)
n_ins_code = len(NEW_complete_code_list)

# DELETE
# primary key that is in the previous version of the table and not in the new one -intersection list-


print("Number of Inserted in the phenotype_effect table where is an indication: ", n_ins_indication, "\nNumber of repeat PK in the phenotype_effect table where is an indication: ",n_same_indication)
print("Number of Inserted in the code table: ", n_ins_code)
