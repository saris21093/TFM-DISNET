"""Disease table.

This module works in order to get disease and drug-disease information and insert them into DISNET drugslayer database.

It fills 2 tables: disease and drug_disease.

The data are extracted from CTD --> Comparative Toxicogenomics Database.

The file CTD_chemicals_diseases contains the relationship between chemicals and diseases, however, only drugs must be chosen.

The identifier of the drugs in CTD is the MeSH code.

In order to get only the drugs, the file CTD_chemicals is used to get the DrugBank id of the drugs, and link them with the MeSh code of the chemical,
then the drugs in the file CTD_chemicals_diseases will be filtered """

import requests
import gzip
import csv
import itertools
import re
import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor


# links where the data are, and their names
url_chemical_diseases = 'http://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz'
file_name_chemical_diseases='CTD_chemicals_diseases.tsv.gz'
url_chemical="http://ctdbase.org/reports/CTD_chemicals.tsv.gz"
file_name_chemical="CTD_chemicals.tsv.gz"

def request_url(url,file_name):
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


# Download the file CTD_chemicals_diseases and CTD_chemicals file, this file contains the drugs and the related diseases 
request_url(url_chemical_diseases,file_name_chemical_diseases)
request_url(url_chemical,file_name_chemical)


# Get the DrugBank codes of each drug from the has_code table, keep them in a diccionary. 
# The  DrugBank id as a key and the chEMBL id as a value 
drugbank_chembl={}
cross_references_list=get_list("SELECT * FROM has_code where entity_id = 2 and resource_id = 95 ")
for i in cross_references_list:
    chembl_code = i[1]
    drugbank_code = i[2]
    drugbank_chembl[drugbank_code]=chembl_code


# Get Previous Version of tables disease and drug_disease
PV_disease_table=get_list("select * from disease")
PV_drug_disease_table=get_list("select *from drug_disease")

# Get the primary keys (pk) from the disease and drug disease Previous Version of tables
PV_PK_disease_table=get_list("select disease_id from disease")
PV_PK_disease_table=list(*zip(*PV_PK_disease_table))
PV_PK_drug_disease_table=get_list("select disease_id, drug_id from drug_disease")


# The identifier of the drugs in CTD is the MeSH code.
# A dicctionary is created in order to keep the relation between a MeSH code with a DrugBank id
meshdrug_drugbank={}
with gzip.open('CTD_chemicals.tsv.gz','rb') as f1:
    for line in itertools.islice(f1, 30, None): # The data start at line 30
        lines=(line.decode().strip().split('\t'))
        if len(lines)>8:
            if re.match(r'^MESH:\w+',lines[1]):
                meshcodes=re.split('MESH:',lines[1])
                meshdrug=meshcodes[1]            
                drugbank_splitcodes=lines[8].split("|")
                drugbank_id=drugbank_splitcodes[0]
                meshdrug_drugbank[meshdrug]=drugbank_id

# Initialize auxiliaries

# Count for keeping the quantity of data that are going to be inserted
count=0

# These Lists are going to keep tuples with the NEW complete data which will be inserted in the tables
NEW_complete_disease_list=[]
NEW_complete_drug_disease_list=[]

# These Lists are going to keep the NEW pk of the data that will be inserted
NEW_disease_list=[]
NEW_drug_disease=[]

# These variables will keep the intersection between old data and new data
intersection_disease = []
intersection_drug_disease  = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_drug_disease = 0
n_ins_disease = 0
n_upd_drug_disease = 0
n_upd_disease = 0
n_del_drug_disease = 0
n_del_disease = 0
n_same_drug_disease = 0
n_same_disease = 0


# Source from data in this file were extracted
SOURCE="CTD"
source_id=int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])

with gzip.open(file_name_chemical_diseases,'rb') as f2:
    for line in itertools.islice(f2, 30, None): # The data start at line 30
        lines=(line.decode("UTF-8").strip().split('\t'))
        curated = lines[5]
        inferred = lines[7]
        chemical_id = lines[1]
        disease_old_id = lines[4] #The disease id OMIM or MeSH
        disease_name = 	lines[3]
        
        # some drug-disease association has information about Direct Evidence	      
      



        
        if chemical_id in meshdrug_drugbank:
            if (re.match(r'^MESH:\w+',disease_old_id)):
                meshcodes=re.split('MESH:',disease_old_id)
                disease_id=meshcodes[1]
                reference_id= 75
                
            if (re.match(r'^OMIM:\w+',disease_old_id)):
                omimcodes=re.split('OMIM:',disease_old_id)
                disease_id=omimcodes[1]
                reference_id= 72
            
            # If the chemical_id(MeSH) has a DrugBank_id  is because is a drug
            drugbank_id=meshdrug_drugbank[chemical_id]
            # Check if the drug is in our database
            if drugbank_id in drugbank_chembl:
                if curated:
                    inference_score = None
                    if curated == "therapeutic":
                        direct_evidence='T'
                    if curated == "marker/mechanism":
                        direct_evidence='M'

                else:
                    direct_evidence = '-'
                    inference_score = inferred

                

                drug_id=drugbank_chembl[drugbank_id]
                disease=(reference_id, disease_id, disease_name)
                drug_disease_pk = (disease_id, drug_id)
                drug_disease=(disease_id, drug_id, source_id, direct_evidence, inference_score)


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
                
                if not disease_id in PV_PK_disease_table:
                    
                    if not disease_id in NEW_disease_list: 
                        NEW_disease_list.append(disease_id) 
                        NEW_complete_disease_list.append(disease) 
                        n_ins_disease += 1
                
                else: 
                    if not disease_id in intersection_disease:
                        intersection_disease.append(disease_id) # Add the pk that is in the previous and the actual version
                        n_same_disease +=1 
                    for rows in PV_disease_table:
                        PV_disease_id=rows[1]
                        PV_disease_name = rows[2]
                        if disease_id == PV_disease_id:
                            if PV_disease_name != disease_name:
                                disease_update_values = (disease_name, disease_id)
                                cursor.execute("UPDATE disease SET disease_name = '%s' where disease_id = '%s'" % disease_update_values)
                                n_upd_disease += 1
                                
                # Drug - Disease
                
                if not drug_disease_pk in PV_PK_drug_disease_table: 
                    if not  drug_disease_pk in NEW_drug_disease: 
                        

                        NEW_drug_disease.append(drug_disease_pk) 
                        NEW_complete_drug_disease_list.append(drug_disease) 
                        count+=1
                        n_ins_drug_disease+=1
                        

                else:
                    if not  drug_disease_pk in intersection_drug_disease:
                        intersection_drug_disease.append(drug_disease_pk) # Add the pk that is in the previous and the actual version
                        n_same_drug_disease += 1
                    for rows in PV_drug_disease_table:
                        if disease_id == rows[0] and drug_id == rows[1]:
                            if direct_evidence != '-':
                                if direct_evidence != rows[3]:
                                    
                                    drug_disease_update_values = (direct_evidence, disease_id, drug_id)
                                    cursor.execute("UPDATE drug_disease SET direct_evidence = '%s' where disease_id = '%s' and drug_id = '%s' " % drug_disease_update_values)
                                    n_upd_drug_disease +=1
                            if inference_score != None and rows[4]!=None:
                                inference_score = rows[4]
                                drug_disease_update_values = (inference_score, disease_id, drug_id)
                                cursor.execute("UPDATE drug_disease SET inferred_score = '%s' where disease_id = '%s' and drug_id = '%s' " % drug_disease_update_values)
                                n_upd_drug_disease +=1

                                
                if count == 500:
                    cursor.executemany("insert into disease values(%s,%s,%s)",NEW_complete_disease_list)
                    NEW_complete_disease_list=[]
                    cursor.executemany("insert into drug_disease values(%s,%s,%s,%s,%s)",NEW_complete_drug_disease_list)
                    NEW_complete_drug_disease_list=[]
                    count=0
                    

cursor.executemany("insert into disease values(%s,%s,%s)",NEW_complete_disease_list)
cursor.executemany("insert into drug_disease values(%s,%s,%s,%s,%s)",NEW_complete_drug_disease_list)


# DELETE
# primary key that is in the previous version of the table and not in the new one -intersection list-

for PV_disease_id in PV_PK_disease_table:
    if not PV_disease_id in intersection_disease:
        cursor.execute("DELETE FROM disease WHERE disease_id = '%s'" % PV_disease_id)
        n_del_disease +=1

for row in PV_PK_drug_disease_table:
    PV_drug_disease_pk = (row[0],row[1])
    if not PV_drug_disease_pk in intersection_drug_disease:
        cursor.execute("DELETE FROM drug_disease WHERE disease_id = '%s' and drug_id = '%s'" % PV_drug_disease_pk)
        n_del_drug_disease += 1


print("Number of Inserted in the disease table: ", n_ins_disease, "\nNumber of Updates in the disease table: ", n_upd_disease, "\nNumber of Deletes in the disease table:  ",n_del_disease, "\nNumber of repeat PK in the disease table: ",n_same_disease)
print("Number of Inserted in the drug_disease table: ", n_ins_drug_disease, "\nNumber of Updates in the drug_disease table: ", n_upd_drug_disease, "\nNumber of Deletes in the drug_disease table:  ",n_del_drug_disease, "\nNumber of repeat PK in the drug_disease table: ",n_same_drug_disease)
