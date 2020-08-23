"""Cross Reference.

This module is for getting the UMLS code and ORPHAN code for the diseases.

it fills 2 tables: code and has_code.

The UMLS code are extracted with an API from UMLS Terminology Services REST API.

The ORPHAN code are extracted with an json file from The web page http://www.orphadata.org.
"""

import get_orhpan_code
import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor

# Initialize auxiliaries

# Counts for keeping the quantity of data that are going to be inserted
count_umls=0
count_orphanet=0

# These Lists are going to keep the pk of the data that will be inserted
code_umls_list=[]
has_code_umls_list=[]
code_orphan_list=[]
has_code_orphan_list=[]

# These Lists are going to keep tuples with the data which will be inserted in the tables
new_list_code_umls=[]
new_list_code_orphan=[]
new_list_has_code_umls = []

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_code = 0
n_ins_orphan=0
n_ins_orphan_umls=0


# Get the list of the diseases
disease_list=get_list("select resource_id, disease_id, disease_name from disease")

# Get orphan Dic Mesh
dic={}
orphan_dic=get_orhpan_code.orphan_codes(dic,'MeSH')

# Get the list of Phenotype_effect
phenotype_effect = get_list("select distinct phenotype_id from phenotype_effect")
phenotype_effect=list(*zip(*phenotype_effect))

# Get orphan Dic UMLS
dic2={}
orphan_dic_UMLS=get_orhpan_code.orphan_codes(dic2,'UMLS')

# Get the list of the ids which have already UMLS id
has_code_table=get_list(" select id from has_code where resource_id = 121")
has_code_table=list(*zip(*has_code_table))

code_table=get_list(" select code from code where resource_id = 121")
code_table=list(*zip(*code_table))

code_table_orphan=get_list(" select code from code where resource_id = 99")
code_table_orphan=list(*zip(*code_table_orphan))



# id_resoruce_id ---> MESH
ID_RESOURCE_ID = 75

# Resource id
UMLS_RESOURCE_ID = 121
ORPHAN_RESOURCE_ID = 99

# Entity id
DISEASE_ENTITY_ID = 1


for i in disease_list:
    disease_id = i[1]
    disease_name = i[2]

    # There are two different codes in the disease table, OMIM and MeSH
    if i[0] == 72:
        source_cr= 'OMIM'
    if i[0] == 75:
        source_cr = 'MSH'

    # in order to avoid duplicate primary key of the tables, it keeps all the keys in a list,
    # Other list keeps tuples with the data which will be inserted in the tables, 
    # They will be inserted each 500 rows.
    # This method is faster than handle the exception and insert data one per one

    if not disease_id in has_code_table:
        umls_id=get_umls.get_umls(disease_id,disease_name,source_cr)
        if umls_id != None:    
            has_code_pk=(disease_id,umls_id)
            if not umls_id in code_table:
                if not umls_id in new_list_code_umls:
                    new_list_code_umls.append(umls_id)
                    new_list_has_code_umls.append(has_code_pk)
                    code_umls=(umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                    has_code_umls=(ID_RESOURCE_ID,disease_id,umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                    code_umls_list.append(code_umls)
                    has_code_umls_list.append(has_code_umls)
                    count_umls+=1
                    n_ins_code += 1
                else:
                    if not has_code_pk in new_list_has_code_umls:
                        new_list_has_code_umls.append(has_code_pk)
                        has_code_umls=(ID_RESOURCE_ID,disease_id,umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                        has_code_umls_list.append(has_code_umls)
                        count_umls+=1
                        n_ins_code += 1
            else:
                if not has_code_pk in new_list_has_code_umls:
                    new_list_has_code_umls.append(has_code_pk)
                    has_code_umls=(ID_RESOURCE_ID,disease_id,umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                    has_code_umls_list.append(has_code_umls)
                    count_umls+=1
                    n_ins_code += 1


             
        
    if disease_id in orphan_dic:
        orphan_id=orphan_dic[disease_id]
        if orphan_id != None:
            if not orphan_id in code_table_orphan:
                if not orphan_id in new_list_code_orphan:
                    new_list_code_orphan.append(orphan_id)
                    code_orphan=(orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    has_code_orphan=(ID_RESOURCE_ID,disease_id,orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    code_orphan_list.append(code_orphan)
                    has_code_orphan_list.append(has_code_orphan)
                    count_orphanet+=1
                    n_ins_orphan +=1


        
        if count_umls==10:
            cursor.executemany("insert into code values(%s,%s,%s)",code_umls_list)
            cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",has_code_umls_list)
            code_umls_list=[]
            has_code_umls_list=[]
            count_umls=0

        if count_orphanet==500:
            cursor.executemany("insert into code values(%s,%s,%s)",code_orphan_list)
            cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",has_code_orphan_list)
            code_orphan_list=[]
            has_code_orphan_list=[]
            count_orphanet=0

cursor.executemany("insert into code values(%s,%s,%s)",code_umls_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",has_code_umls_list)
cursor.executemany("insert into code values(%s,%s,%s)",code_orphan_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",has_code_orphan_list)

print("Number of Inserted in the code -UMLS- table: ", n_ins_code)
print("Number of Inserted in the code -Orphan- table: ", n_ins_orphan)

# This is for there are some phenotype id that has ORPHAN code
code_table_orphan=get_list(" select code from code where resource_id = 99")
code_table_orphan=list(*zip(*code_table_orphan))
count_orphanet = 0
for i in phenotype_effect:
    if i in orphan_dic_UMLS:
        orphan_id=orphan_dic_UMLS[i]
        if orphan_id != None:
         
            if not orphan_id in code_table_orphan:
                if not orphan_id in new_list_code_orphan:
                    new_list_code_orphan.append(orphan_id)
                    code_orphan=(orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    ID_RESOURCE_ID_UMLS = 121
                    has_code_orphan=(ID_RESOURCE_ID_UMLS,disease_id,orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    code_orphan_list.append(code_orphan)
                    has_code_orphan_list.append(has_code_orphan)
                    count_orphanet+=1
                    n_ins_orphan_umls +=1


cursor.executemany("insert into code values(%s,%s,%s)",code_orphan_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s,%s)",has_code_orphan_list)


print("Number of Inserted in the code -Orphan- from phenotype table: ", n_ins_orphan_umls)

