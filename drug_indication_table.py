"""Drug Indication.

This module works in order to get the information about the relation of the drugs and the indication of the drugs and insert them into DISNET drugslayer database.

It fills 1 table: drug_phenotype_effect.

All the data are extracted from chEMBL python client.
"""


import requests
import gzip
import csv
import itertools
import re
import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor


# Get the de MeSH code and the UMLS code from the Previous Version of has_code table, and create a dictionary with MeSH as key and Code as  value
PV_has_code_table=get_list("select id,code from has_code where resource_id = 121")
mesh_umls_dic = {}

for i in PV_has_code_table:
    mesh_code=i[0]
    umls_code=i[1]
    mesh_umls_dic[mesh_code]=umls_code
        
# Get the parent table (drug) from the database in order to obtain the indication just for the approved ones, these data will be keep in a list        
parent_drug_table=get_list("select distinct drug_id from drug")
parent_drug_table=list(*zip(*parent_drug_table))

# Get the primary keys (pk) from drug_phenotype_effect table Previous Version
PV_PK_drug_pe_table = get_list("select phenotype_id, drug_id, source_id from drug_phenotype_effect where phenotype_type = 'INDICATION'")

# Get the Previous Version of drug_phenotype_effect table
PV_drug_pe_table = get_list("select * from drug_phenotype_effect where phenotype_type = 'INDICATION'")

# Initialize auxiliaries

# This List is going to keep tuples with the NEW complete data which will be inserted in the tables
NEW_complete_drug_indication_list=[]

# This List is going to keep the NEW pk of the data that will be inserted
NEW_drug_indication_list = []

# This List  will keep the intersection between old data and new data
intersection_drug_indication = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_drug_indication = 0
n_upd_drug_indication = 0
n_del_drug_indication = 0
n_same_drug_indication = 0

# Source from data in this file were extracted
SOURCE = "CHEMBL"
# Get the source_id of SOURCE from the table "source"
source_id = int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])

# Get the info from chEMBL Python Client
from chembl_webresource_client.new_client import new_client
indications = new_client.drug_indication

for i in indications:
    indication_mesh=i['mesh_id']
    drug_id = i['molecule_chembl_id']
    max_phase = float(i['max_phase_for_ind'])
    phenotype_type="INDICATION"
    
    # INSERT: The foreign keys have to be previously in the database.
    # If the indication_mesh is already in the code and has_code table,
    # get the umls code from the dictionary created before.
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
    

    if drug_id in parent_drug_table:
        if indication_mesh in mesh_umls_dic.keys():
            indication_umls = mesh_umls_dic[indication_mesh]

            drug_indication_pk=(indication_umls,drug_id,source_id)
            drug_indication = (indication_umls,drug_id,source_id,max_phase,phenotype_type)

            if not drug_indication_pk in PV_PK_drug_pe_table:
                if not drug_indication_pk in NEW_drug_indication_list:
                    NEW_drug_indication_list.append(drug_indication_pk)
                    NEW_complete_drug_indication_list.append(drug_indication)
                else:
                    index = NEW_drug_indication_list.index(drug_indication_pk)
                    if NEW_complete_drug_indication_list[index][3] < max_phase:
                        NEW_complete_drug_indication_list.pop(index)
                        NEW_drug_indication_list.pop(index)
                        drug_indication=(indication_umls,drug_id,source_id,max_phase,phenotype_type)
                        NEW_complete_drug_indication_list.append(drug_indication)
                        NEW_drug_indication_list.append(drug_indication_pk)
            else:
                if not drug_indication_pk in intersection_drug_indication:
                    n_same_drug_indication +=1
                    intersection_drug_indication.append(drug_indication_pk) # Add the pk that is in the previous and the actual version
                for row in PV_drug_pe_table:
                    PV_drug_indication_phenotypeid = row[0]
                    PV_drug_indication_drug_id = row[1]
                    PV_max_phase = row[3]
                    PV_phenotype_type = row[4]
                    if indication_umls == PV_drug_indication_phenotypeid and drug_id == PV_drug_indication_drug_id and phenotype_type == PV_phenotype_type:
                        if max_phase > PV_max_phase:
                            drug_indication_update_values = (max_phase,indication_umls ,drug_id,phenotype_type)
                            cursor.execute("UPDATE drug_phenotype_effect SET score = '%s' where (phenotype_id = '%s') and (drug_id = '%s') and (phenotype_type = '%s')" % drug_indication_update_values)
                            n_upd_drug_indication +=1
# INSERT the data to the database
cursor.executemany("insert into drug_phenotype_effect values(%s,%s,%s,%s,%s)",NEW_complete_drug_indication_list)
n_ins_drug_indication = len(NEW_complete_drug_indication_list)

# DELETE
# primary key that is in the previous version of the table and not in the new one -intersection list-
for row in PV_drug_pe_table:
    PV_indication_pk = (row[0],row[1],row[2])
    if not PV_indication_pk in intersection_drug_indication:
        cursor.execute("DELETE FROM drug_phenotype_effect WHERE phenotype_id = '%s' and drug_id = '%s' and source_id =  '%s' " % PV_indication_pk)
        n_del_drug_indication +=1

print("Number of Inserted in the drug_phenotype_effect table where is an indication: ", n_ins_drug_indication, "\nNumber of Updates in the drug_phenotype_effect table where is an indication: ", n_upd_drug_indication, "\nNumber of Deletes in the drug_phenotype_effect table where is an indication:  ",n_del_drug_indication, "\nNumber of repeat PK in the drug_phenotype_effect table where is an indication: ",n_same_drug_indication)

      
                