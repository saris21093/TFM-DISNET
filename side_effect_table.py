"""Side Effect and Drug - Side Effect

This module works in order to get side effect information and insert them into DISNET drugslayer database.

It fills 2 tables: phenotype_effect and drug_phenotype_effect.

The data are extracted from SIDER --> Side Effect Resource http://sideeffects.embl.de/

The SIDER Database has a different identifiers for the drugs --> STITCH,

Therefore this needs to be changed to chEMBL. However they do not have any other identifier in the relationship between chemicals and disease file.

So the way to solve this problem is to download a file called drug_atc, this allows to map the STITCH id to ATC code, that already is kept in the database.
"""
import requests
import gzip
import csv
import itertools
import re
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor


# Get Previos Version of tables phenotype_effect and drug_phenotype_effect where source_id = 2
# pe --> phenotype effect

PV_pe_table = get_list("select * from phenotype_effect")
PV_drug_pe_table = get_list("select * from drug_phenotype_effect where source_id =2")

# Get the primary keys (pk) of phenotype effect and drug- phenotype effect Previous Version tables 
PV_PK_pe_table = get_list("select phenotype_id from phenotype_effect")
PV_PK_pe_table=list(*zip(*PV_PK_pe_table))
PV_PK_drug_pe_table = get_list("select phenotype_id, drug_id, source_id from drug_phenotype_effect where source_id = 2")


# links where the data are, and their names
url_drug_atc = 'http://sideeffects.embl.de/media/download/drug_atc.tsv'
file_name_drug_atc='drug_atc.tsv'

url_side_effects = 'http://sideeffects.embl.de/media/download/meddra_freq.tsv.gz'
file_name_side_effects='meddra_freq.tsv.gz'

def download_file(url,file_name):
    """This Function make the request to the link where the files are,
    and keep the information in a new file.

    Args:
        url (str): url where the file are.
        file_name (str): the name of the new file.

    Returns:
        The files downloaded.
    """
    myfile = requests.get(url)
    if myfile.status_code == 200:
        open(file_name, 'wb').write(myfile.content)
    else:
        print('The link does not work')

# Download the file drug_atc and meddra_freq file, this file contains the drugs and their side effects
download_file(url_drug_atc,file_name_drug_atc)
download_file(url_side_effects,file_name_side_effects)

# Create a diccionary in order to keep the information, ATC codes as a key, and the STITCH as values.
def atc_stitch_codes(file_atc):
    """ This function takes the drug_atc file in order to create a diccionary
     in order to keep the information, ATC codes as a key, and the STITCH as values.
     
    Args:
        file_atc (str): The name of the file where the information about atc codes is.

    Returns:
        atc_stitch (dic): A diccionary with the cross-reference between atc and stitch

     """
    atc_stitch={}
    with open(file_atc) as tsvfile:
        atc_codes = csv.reader(tsvfile, delimiter="\t")
        for line in atc_codes:
            atc=line[1]
            stitch=line[0]
            atc_stitch[atc]=stitch
    return atc_stitch

# Get the ATC_STITCH
atc_stitch_dic=atc_stitch_codes(file_name_drug_atc)

# Get the ATC codes with its correspond chEMBL id from DISNET database
ATC_code_table=get_list("select drug_id, ATC_code_id from ATC_code")

# iterate through the ATC_code_table in order to get a diccionary with STITCH id as keys and chEMBL id as values.
chembl_stitch_dic={}
for i in ATC_code_table:
    drug_id=i[0]
    atc_code=i[1]
    if atc_code in atc_stitch_dic:
        chembl_stitch_dic.setdefault(atc_stitch_dic[atc_code],drug_id)

# Initialize auxiliaries
# se --> side effect

# These Lists are going to keep the pk of the data that will be inserted
NEW_se_list=[]
NEW_drug_se_list=[]

# These Lists are going to keep tuples with the data which will be inserted in the tables
NEW_complete_se=[]
NEW_complete_drug_se=[]   

# These variables will keep the intersection between old data and new data
intersection_se=[]
intersection_drug_se=[]

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_se = 0
n_upd_se = 0
n_del_se = 0
n_same_se = 0
n_ins_drug_se = 0
n_upd_drug_se = 0
n_del_drug_se = 0
n_same_drug_se = 0
# Source from data in this file were extracted
SOURCE = "SIDER"
cursor.execute("SELECT source_id from source where name = '%s'" % SOURCE)
source=cursor.fetchall()
source_id=int(source[0][0])

# Open the file with the relationship between drugs and side effects, called file_name_side_effects
# Unzip and read the file
with gzip.open(file_name_side_effects,'rb') as f:
    for lines in itertools.islice(f,0,None):
        line=(lines.decode().strip().split('\t'))
        stitch_code = line[0]
        concept_type = line[7]
        if concept_type =='PT': # Filter by PT, preferer term in the line 7 called MedDRA concept type
            if stitch_code in chembl_stitch_dic: #Filter if STITCH code is in the ch_st dictionary
                drug_id=chembl_stitch_dic[stitch_code]
                se_id = (line[8]) 
                frec_se=(float(line[6]))
                phenotype_type="SIDE EFFECT"
                se_name=line[9]
                se_name = se_name.replace("'", "\\'") # Avoid the problem with the "'" at the moment to insert the data to the Database we replace it by "\' "
               
                se = (se_id,se_name) # side effect data
                drug_se_pk = (se_id,drug_id,source_id) # Primary keys drug_phenotype_effect
                drug_se =(se_id,drug_id,source_id,frec_se,phenotype_type) # drug side effect data
                
                # INSERT: primary key (pk) that is not in the previous version
                # Sometimes there are repeat pk in the new data, 
                # in order to avoid duplicate primary key of the tables,
                # it keeps all the pk in a list.
                # Other list keeps tuples with the data which will be inserted in the tables, 
                # This method is faster than handle the exception and insert data one per one. 

                # UPDATE: primary key is that is in the previous version of the table
                # If all the data is the same it is repeat data
                # If the data is different is an update

                if not se_id in PV_PK_pe_table:
                    if not se_id in NEW_se_list:
                        NEW_se_list.append(se_id)
                        NEW_complete_se.append(se) 
                else:
                    n_same_se+=1
                   
                # Drug-side effect 
                # INSERT: primary key (pk) that is not in the previous version
                # Sometimes there are repeat pk in the new data, in this case.
                # There are several drug-phenotype combination with different frecuency, and
                # only the higher one will be taken.
                # in order to avoid duplicate primary key of the tables and choose the higher frecuency:
                # it keeps all the pk in a list.
                # Other list keeps tuples with the data which will be inserted in the tables.
                # If the pk is already in the pk list, it takes the index where the pk is in the list.
                # If the new frecuency is higher, the row in the index will be delete from the list with the tuple with all the information
                # and the list with the pk.
                # This method is faster than handle the exception and insert data one per one. 

                # UPDATE: primary key that is in the previous version of the table and in the new one
                # If all the data is the same it is repeat data
                # If the data is different is an update
              
                        
                if not drug_se_pk in PV_PK_drug_pe_table:
                    if not drug_se_pk in NEW_drug_se_list:
                        NEW_drug_se_list.append(drug_se_pk)
                        NEW_complete_drug_se.append(drug_se)
                        
                    if drug_se_pk in NEW_drug_se_list:
                        index=NEW_drug_se_list.index(drug_se_pk)
                        if NEW_complete_drug_se[index][3] < frec_se:
                            NEW_complete_drug_se.pop(index)
                            NEW_drug_se_list.pop(index)  
                            drug_se=(se_id,drug_id,source_id,frec_se,phenotype_type)
                            NEW_complete_drug_se.append(drug_se)
                            NEW_drug_se_list.append(drug_se_pk)
                    
                else:
                    if not drug_se_pk in intersection_drug_se:
                        intersection_drug_se.append(drug_se_pk) # Add the pk that is in the previous and the actual version
                        n_same_drug_se+=1
                    for row in PV_drug_pe_table:
                        PV_drug_se_phenotypeid = row[0]
                        PV_drug_se_drug_id = row[1]
                        PV_frec = row[3]
                        PV_phenotype_type = row[4]
                        if se_id == PV_drug_se_phenotypeid and drug_id == PV_drug_se_drug_id and phenotype_type == PV_phenotype_type:
                            if frec_se > PV_frec:
                                dse_update_values = (frec_se,se_id ,drug_id,phenotype_type)
                                cursor.execute("UPDATE drug_phenotype_effect SET score = '%s' where (phenotype_id = '%s') and (drug_id = '%s') and (phenotype_type = '%s')" % dse_update_values)
                                n_upd_drug_se +=1
# INSERT the data to the database
cursor.executemany("insert into phenotype_effect values(%s,%s)",NEW_complete_se)
cursor.executemany("insert into drug_phenotype_effect values(%s,%s,%s,%s,%s)",NEW_complete_drug_se)
n_ins_se = len(NEW_complete_se)
n_ins_drug_se=len(NEW_complete_drug_se)

# DELETE
# primary key that is in the previous version of the table and not in the new one -intersection list-


for row in PV_drug_pe_table: 
    PV_dse_pk = (row[0], row[1], row[2])
    if not PV_dse_pk in intersection_drug_se:
        cursor.execute("DELETE FROM drug_phenotype_effect WHERE phenotype_id = '%s' and drug_id =  '%s' and phenotype_type = '%s'" % PV_dse_pk)
        n_del_drug_se += 1

print("Number of Inserted in the phenotype_effect table where is a side effect: ", n_ins_se, "\nNumber of repeat PK in the phenotype_effect table where is a side effect: ",n_same_se)
print("Number of Inserted in the drug_phenotype_effect table where is a side effect: ", n_ins_drug_se, "\nNumber of Updates in the drug_phenotype_effect table where is a side effect: ", n_upd_drug_se, "\nNumber of Deletes in the drug_phenotype_effect table where is a side effect:  ",n_del_drug_se, "\nNumber of repeat PK in the drug_phenotype_effect table where is a side effect: ",n_same_drug_se)
