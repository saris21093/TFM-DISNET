"""Target Table.

This module works in order to get target and UniProt code of the target and insert them into DISNET drugslayer database.

It fills 3 tables: target, code and has_code.

All the data are extracted from chEMBL python client.
"""

import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor

# Get Previous Version of target table    
PV_target_table = get_list("select * from target")

# get column length
column_length=len(PV_target_table[0])

# Get the primary keys (pk) from Previous Version of target table
PV_PK_target_table = get_list("select target_id from target")
PV_PK_target_table=list(*zip(*PV_PK_target_table))

# Get the primary keys (pk) from Previous Version of code table
PV_PK_code_table=get_list("select code from code where resource_id = 86")
PV_PK_code_table=list(*zip(*PV_PK_code_table))

# Get columns names
columns_names = get_list("""SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA LIKE 'disnet_drugslayer' AND TABLE_NAME = 'target' """)


#Initialize auxiliaries and lists

# Counts for keeping the quantity of data that are going to be inserted
count=0
count_code=0

# These Lists are going to keep tuples with the NEW complete data which will be inserted in the tables
NEW_complete_target_list=[]
NEW_complete_code_list=[]
NEW_complete_has_code_list=[]

# These Lists are going to keep the NEW pk of the data that will be inserted
NEW_code_list=[]
NEW_target=[]

# These variables will keep the intersection between old data and new data
intersection_target = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_target = 0
n_ins_code = 0
n_upd_target = 0
n_del_target = 0
n_same_target = 0

# Source from data in this file were extracted
SOURCE = "CHEMBL"

# Get the source_id of SOURCE from the table "source"
source_id = int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])

# id_resource_id --> CHEMBL
ID_RESOURCE_ID = 97

# Resource id
# UP --> UniProt
UP_RESOURCEID = 86

# Entity id
TARGET_ENTITYID = 3

# Get the info from chEMBL Python Client
from chembl_webresource_client.new_client import new_client
drug_target_chembl = new_client.target
for i in drug_target_chembl:
    target_id=i['target_chembl_id']
    preferred_name=i['pref_name']
    target_type=i['target_type']
    organism=i['organism']
    tax_id= i['tax_id']
    
    # Some targets do not have accession
    if i['target_components']:
        accession=i['target_components'][0]['accession']
    else:
        accession=None
        
    target=(target_id,preferred_name,target_type,organism,tax_id)
    
    # INSERT: primary key (pk) that is not in the previous version
    # Sometimes there are repeat pk in the new data, 
    # in order to avoid duplicate primary key of the tables,
    # it keeps all the pk in a list.
    # Other list keeps tuples with the data which will be inserted in the tables, 
    # They will be inserted each 500 rows.
    # This method is faster than handle the exception and insert data one per one. 
    
    # UPDATE: primary key is that is in the previous version of the table
    # If all the data is the same it is repeat data
    # If the data is different is an update
    
    
    if not target_id in PV_PK_target_table:
        if not target_id in NEW_target:
            NEW_target.append(target_id)
            NEW_complete_target_list.append(target)
            count+=1
            n_ins_target += 1
    else:
        intersection_target.append(target_id) # Add the pk that is in the previous and the actual version 
        n_same_target += 1
        for row in PV_target_table:
            PV_target_id = row[0]
            if target_id == PV_target_id:
                for column in range(1,column_length):
                    if row[column] != target[column]:
                        target_update_values = (columns_names[column][0],target[column],target_id)
                        cursor.execute("UPDATE target SET %s = '%s' where target_id = '%s'" % target_update_values)
                        n_upd_target += 1
    # Insert the Data each 500 rows in each list
    if count==500:
        cursor.executemany("insert into target values(%s,%s,%s,%s,%s)",NEW_complete_target_list)
        NEW_complete_target_list=[]
        count=0

    # Get the UniProt Code for the target
    # Insert it into the code and has_code tables
    if accession != None:
        if not accession in PV_PK_code_table:
            if not accession in NEW_code_list:
                NEW_code_list.append(accession)
                code_accession=(accession,UP_RESOURCEID,TARGET_ENTITYID)
                has_code=(ID_RESOURCE_ID,target_id,accession,UP_RESOURCEID,TARGET_ENTITYID)
                NEW_complete_code_list.append(code_accession)
                NEW_complete_has_code_list.append(has_code)
                count_code+=1
                n_ins_code += 1
       
        # Insert the Data each 500 rows in each list
        if count_code==500:
            cursor.executemany("insert into code values(%s,%s,%s)",NEW_complete_code_list)
            cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",NEW_complete_has_code_list)
            NEW_complete_code_list=[]
            NEW_complete_has_code_list=[]
            count_code=0


# Insert the remaining data in the lists
cursor.executemany("insert into target values(%s,%s,%s,%s,%s)",NEW_complete_target_list)
cursor.executemany("insert into code values(%s,%s,%s)",NEW_complete_code_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",NEW_complete_has_code_list)

# DELETE:
# primary key that is in the previous version of the table and not in the new one -intersection list-
for PV_target_id in PV_PK_target_table:
    if not PV_target_id in intersection_target:
        cursor.execute("DELETE FROM target WHERE target_id = '%s'" % PV_target_id)
        n_del_target += 1

print("Number of Inserted in the target table: ", n_ins_target, "\nNumber of Updates in the target table: ", n_upd_target, "\nNumber of Deletes in the target table:  ",n_del_target, "\nNumber of repeat PK in the target table: ",n_same_target)
print("Number of Inserted in the code table: ", n_ins_code)

        
        