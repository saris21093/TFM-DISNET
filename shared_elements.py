"""shared elements script.

This script works in order to get the information about phenotypical and biological features for each orphan and rare disease
It also evaluates the shared elements of each orphan disease against all the present diseases in DISNET through Tanimoto/Jaccard index.

"""
import mysql.connector
from mysql.connector import errorcode

# :::::::::DRUG'S LAYER DATABASE CONECTION::::::::::::::::

DB_NAME = 'disnet_drugslayer'


cnx = mysql.connector.connect(host='138.4.130.153',
                            port= '30604'   ,
                            user='drugslayer',
                            password='drugs1234',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True

# Diseases with asociated drugs from the drug's layer
cursor.execute("""select distinct has_code.code from drug_disease
 inner join has_code on has_code.id = drug_disease.disease_id 
 where resource_id = 121""")
drug_disease=cursor.fetchall()
drug_disease=list(*zip(*drug_disease))

#:::::: BIOLOGICAL LAYER CONECTION:::::::::::::::::::::

DB_NAME = 'disnet_biolayer'

cnx = mysql.connector.connect(host='138.4.130.153',
                            port= '30604'   ,
                            user='disnet_user',
                            password='disnet_user2020',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True

# All diseases from the biological layer
cursor.execute("""select disease_id, disease_name from disease """)
disease_names = cursor.fetchall()

# 10 orphan diseases selected
diseases=['C1866398','C0263579','C0751540','C0015708','C0026363','C0026703','C0242342','C0036391','C3495427','C0023520']


#:::::::::::::::::::::::.protein - protein interaction query::::::::::::::::::::::::::::::::

cursor.execute("""SELECT DISTINCT dp1.disease_id, ppi.protein1_id, ppi.protein2_id
FROM  ppi
INNER JOIN (SELECT DISTINCT disease_id, protein_id
FROM disease_gene gd INNER JOIN encodes en
ON ( gd.gene_id = en.gene_id )) as dp1
ON ( dp1.protein_id = ppi.protein1_id )
INNER JOIN (SELECT DISTINCT disease_id, protein_id
FROM disease_gene gd INNER JOIN encodes en
ON ( gd.gene_id = en.gene_id )) as dp2
ON ( dp2.protein_id = ppi.protein2_id AND dp1.disease_id = dp2.disease_id) """)

ppi=cursor.fetchall()

# convert the list of tuples to dictionaries
def ConvertPPI(tup, dic):
    """This Function convert the list of tuples to dictionaries

    Args:
        tup (tuple): the obtained tuple from database
        dic (dict): The name of the new dictionary.

    Returns:
        Dictionary --> dic = {disease1:[[p1,p2],[p2,p3]]}.
    """
    for a, b,c in tup:
        l=[b,c]
        dic.setdefault(a,[]).append(l) 
    return dic


ppis = {} 
ConvertPPI(ppi,ppis)

# Orphan diseases with information of PPIS
ppis_disease={}
for i in diseases:
    if i in ppis:
        ppis_disease[i]=ppis[i]


# Shared elements (PPIS)
cont=0
results={}
shared_PPIS={}
for disease in ppis_disease:
    number_elements_2 = 0
    number_elements=len(ppis_disease[disease])
    results={}
    for one_disease in ppis:
        cont=0
        for elements in ppis_disease[disease]:
            if elements in ppis[one_disease]:
                number_elements_2=len(ppis[one_disease])
                cont+=1
        percentage_shared_elements=round(cont/(number_elements+number_elements_2-cont),2)
        if percentage_shared_elements >=0.8:
            if one_disease in drug_disease:
                results[one_disease]=percentage_shared_elements
                shared_PPIS[disease]=results


#::::::::END PROTEIN PROTEIN INTERACTIONS:::::::::::::::::::::.

#::::::::GENES, PROTREINS, PATHWAYS AND SYMPOTOMS::::::::::::::

def Convert(tup, dic):
    """This Function convert the list of tuples to dictionaries

    Args:
        tup (tuple): the obtained tuple from database
        dic (dict): The new dictionary.

    Returns:
        dic (dic) --> dic = {disease1:[g1,g2]}.
    """

    for a, b in tup:
        dic.setdefault(a,[]).append(b) 
    return dic

# Orphan diseases with information of genes, proteins, pathways and symptoms
def disease_dic(diseases,element):
    """This Function evaluates if the orphan disease has 
    information related to genes, proteins, pathways and symptoms

    Args:
        diseases (list): the list of thr selected orphan diseases
        element (dict): The dictionary of the element.

    Returns:
        Dic (Dict) --> genes = {orphan_disease:[g1,g2]}.
    """
    dic={}
    for i in diseases:
        if i in element:
            dic[i]=element[i]
    return dic

def percentaje_shared_elements(element_disease,element):
    """This Function evaluates the shared elements between 
    the selected orphan diseases and all the diseases

    Args:
        element_disease (dic): The dictionary where the data will be store
        element (dict): The dictionary of the element.

    Returns:
        Dictionary --> shared_genes = {orphan_dis1:{similar_dis:0.5}}.
    """
    cont=0
    results={}
    shared_elements={}
    for disease in element_disease:
        number_elements_2 = 0
        number_elements=len(element_disease[disease])
        results={}
        for one_disease in element:
            cont=0
            for elements in element_disease[disease]:
                if elements in element[one_disease]:
                    number_elements_2=len(element[one_disease])
                    cont+=1
            percentage_shared_elements=round(cont/(number_elements+number_elements_2-cont),2)
            if percentage_shared_elements >=0.8:
                
                if one_disease in drug_disease:
                    results[one_disease]=percentage_shared_elements
                    shared_elements[disease]=results
    return shared_elements

#::::::::GENES::::::::::::::::::::::.
cursor.execute("""SELECT disease_id, gene_id from disease_gene """)
gene=cursor.fetchall()
genes = {} 
Convert(gene, genes)
genes_diseases={}
genes_diseases=disease_dic(diseases,genes)
genes_shared = percentaje_shared_elements(genes_diseases,genes)

#::::::::END GENES:::::::::::::::::::::

#::::::::PROTEINS::::::::::::..........:
cursor.execute("""SELECT DISTINCT disease_id, protein_id
FROM disease_gene gd INNER JOIN encodes en
ON ( gd.gene_id = en.gene_id ) """)
protein=cursor.fetchall()
proteins= {}
Convert(protein,proteins)
proteins_diseases={}
proteins_diseases=disease_dic(diseases,proteins)
proteins_shared = percentaje_shared_elements(proteins_diseases,proteins)
#::::::::END PROTEINS::::::::::::::::::::::

#::::::::PATHWAYS::::::::::::::::::::::::::
cursor.execute("""SELECT DISTINCT disease_id, pathway_id FROM disease_gene gd 
INNER JOIN gene_pathway gp 
ON ( gd.gene_id = gp.gene_id )""")
pathway=cursor.fetchall()
pathways={}
Convert(pathway, pathways)
pathways_diseases={}
pathways_diseases = disease_dic(diseases,pathways)
pathways_shared = percentaje_shared_elements(pathways_diseases,pathways)
#::::::::END PATHWAYS:::::::::::::::::::::::

#::::::::SYMPTOMS:::::::::::::::::::::::::::

#:::::::: PHENOTIPYCAL LAYER CONECTION:::::::::::::::
DB_NAME = 'edsssdb'

cnx = mysql.connector.connect(host='138.4.130.153',
                            port= '30604'   ,
                            user='disnet_user',
                            password='disnet_user2020',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True

cursor.execute("""SELECT DISTINCT lm.cui, hsym.cui AS symptom_cui
FROM disease d
INNER JOIN has_disease hd ON hd.disorder_id = d.disease_id
INNER JOIN document doc ON doc.document_id = hd.document_id AND doc.date = hd.date
INNER JOIN has_source hs ON hs.document_id = doc.document_id AND hs.date = doc.date
INNER JOIN source sce ON sce.source_id = hs.source_id
INNER JOIN has_section hsec ON hsec.document_id = doc.document_id AND hsec.date = doc.date
INNER JOIN has_text ht ON ht.document_id = hsec.document_id AND ht.date = hsec.date AND ht.section_id = hsec.section_id
INNER JOIN has_symptom hsym ON hsym.text_id = ht.text_id
INNER JOIN symptom sym ON sym.cui = hsym.cui
INNER JOIN layersmappings lm ON lm.disnet_id = d.disease_id
WHERE hsym.validated = true """)
symptom=cursor.fetchall()
symptoms={}
Convert(symptom,symptoms)
symptoms_diseases={}
symptoms_diseases = disease_dic(diseases,symptoms)
symptoms_shared = percentaje_shared_elements(symptoms_diseases,symptoms)
#::::::::::END SYMPTOMS::::::::::::::::::::::

file = open("drugs_associated.txt",'w')
       


print(genes_shared)
print(pathways_shared)
print(proteins_shared)
print(symptoms_shared)
print(shared_PPIS)


file.write(str(genes_shared)+'\n')
file.write(str(pathways_shared)+'\n')
file.write(str(proteins_shared)+'\n')
file.write(str(symptoms_shared)+'\n')
file.write(str(shared_PPIS)+'\n')
#::::::::::::::::::::::::::::::::::::::::::::::::.


