"""Drug Target Table.

This module works in order to get the information about the relation of the drugs and the targets of the drugs and insert them into DISNET drugslayer database.

It fills 1 table: drug_target.

All the data are extracted from chEMBL python client.
"""

import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor
      
# Get primary keys (pk) from the parent tables (drug and target)
parent_drug_table=get_list("select distinct drug_id from drug")
parent_drug_table=list(*zip(*parent_drug_table))

parent_target_table=get_list("select distinct target_id from target")
parent_target_table=list(*zip(*parent_target_table))

# Get the Previous Version of drug_target table.
PV_drug_target_table = get_list("select * from drug_target")

# Get the primary keys (pk) of drug_target table Previous Version.
PV_PK_drug_target_table = get_list("select target_id, drug_id from drug_target")


# Initialize auxiliaries

# Count for keeping the quantity of data that are going to be inserted
count=0

# This List is going to keep tuples with the NEW complete data which will be inserted in the tables
NEW_complete_target_list=[]

# This List is going to keep the NEW pk of the data that will be inserted
NEW_drug_target=[]

# This List will keep the intersection between old data and new data
intersection_drug_target = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_drug_target = 0
n_upd_drug_target = 0
n_del_drug_target = 0
n_same_drug_target = 0

# Source from data in this file were extracted
SOURCE = "CHEMBL"

# Get the source_id of SOURCE from the table "source"
source_id=int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])

# Get the info from chEMBL Python Client
from chembl_webresource_client.new_client import new_client
drug_drug_target_chembl = new_client.mechanism

for i in drug_drug_target_chembl:
    target_id=i['target_chembl_id']
    drug_id=i['molecule_chembl_id']
    action_type=i['action_type']

    # INSERT: The foreign keys have to be previously in the database
    # primary key (pk) that is not in the previous version
    # Sometimes there are repeat pk in the new data, 
    # in order to avoid duplicate primary key of the tables,
    # it keeps all the pk in a list.
    # Other list keeps tuples with the data which will be inserted in the tables, 
    # This method is faster than handle the exception and insert data one per one. 

    # UPDATE: primary key is that is in the previous version of the table
    # If all the data is the same it is repeat data
    # If the data is different is an update

    if target_id in parent_target_table:
        if drug_id in parent_drug_table:
            drug_target=(target_id,drug_id,source_id,action_type)
            drug_target_pk = (target_id, drug_id)

            if not drug_target_pk in PV_PK_drug_target_table:
                if not drug_target_pk in NEW_drug_target:
                    NEW_drug_target.append(drug_target_pk)
                    NEW_complete_target_list.append(drug_target)
                    count+=1
                    n_ins_drug_target +=1
            else:
                intersection_drug_target.append(drug_target_pk) # Add the pk that is in the previous and the actual version
                n_same_drug_target +=1
                for row in PV_drug_target_table:
                    PV_drug_target_target_id = row[0]
                    PV_drug_target_drug_id = row[1]
                    PV_action_type = row[3]
                    if target_id == PV_drug_target_target_id and drug_id == PV_drug_target_drug_id :
                        if action_type > PV_action_type:
                            drug_target_update_values = (action_type, target_id ,drug_id)
                            cursor.execute("UPDATE drug_target SET action_type = '%s' where (target_id = '%s') and (drug_id = '%s')" % drug_target_update_values)
                            n_upd_drug_target +=1
            # Insert the Data each 500 rows in each list
            if count==500:
                cursor.executemany("insert into drug_target values(%s,%s,%s,%s)",NEW_complete_target_list)
                NEW_complete_target_list=[]
                count=0
            
# Insert the remaining data in the list
cursor.executemany("insert into drug_target values(%s,%s,%s,%s)",NEW_complete_target_list)

# DELETE:
# primary key that is in the previous version of the table and not in the new one -intersection list-
for row in PV_drug_target_table:
    PV_drug_target_pk = (row[0],row[1])
    if not PV_drug_target_pk in intersection_drug_target:
        cursor.execute("DELETE FROM drug_target WHERE target_id = '%s' and drug_id =  '%s' " % PV_drug_target_pk)
        n_del_drug_target +=1

print("Number of Inserted in the drug_target table: ", n_ins_drug_target, "\nNumber of Updates in the drug_target table: ", n_upd_drug_target, "\nNumber of Deletes in the drug_target table:  ",n_del_drug_target, "\nNumber of repeat PK in the drug_target table: ",n_same_drug_target)
