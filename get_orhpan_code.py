"""Get Orphan Code

This module is for getting the orphan code of the diseases, using the json file from the orphan webpage

The way to get this file is often changing.
"""

import json
import requests
import tarfile
from get_list import get_list


def orphan_codes(orphan_dic,source):
    """This fuction creates a dictionary with the cross reference between ORPHAN code and mesh codes.

    Args:
        orphan_dic (dic): An empty dictionary
        source (str): the source for creating the cross-references dictionary

    Returns:S
        orphan_dic (dic): The dictionary with a cross-reference between CUI UMLS and ORPHAN codes


    """
    url_orphan = 'http://www.orphadata.org/data/export/en_product1.json.tar.gz'
    file_name='en_product1.json.tar.gz'
    myfile = requests.get(url_orphan)
  
    if myfile.status_code == 200:
        open(file_name, 'wb').write(myfile.content)
        tar = tarfile.open(file_name,'r:gz')
        tar.extractall()
        orphan_json = tar.getnames()[0]
    else:
        print('The link does not work')
    
    with open(orphan_json) as file:
        data = json.load(file)
        for key in data:
            a=len(data[key][0]['DisorderList'][0]['Disorder'])
            for i in range(a):
                orphan_number=(data[key][0]['DisorderList'][0]['Disorder'][i]['OrphaCode'])
                counts=data[key][0]['DisorderList'][0]['Disorder'][i]['ExternalReferenceList'][0]['count']
                counts=int(counts)
                name = data[key][0]['DisorderList'][0]['Disorder'][i]['Name'][0]['label']
                if counts >1:
                    for j in range(counts):
                        links = data[key][0]['DisorderList'][0]['Disorder'][i]['ExternalReferenceList'][0]['ExternalReference'][j]
                        if links['Source'] == source:
                            reference=(links['Reference'])
                            orphan_dic[reference]=[[orphan_number][0],name]
                if counts ==1:
                    links=data[key][0]['DisorderList'][0]['Disorder'][i]['ExternalReferenceList'][0]['ExternalReference'][0]
                    if links['Source'] == source:
                        reference=(links['Reference'])
                        orphan_dic[reference]=[[orphan_number][0],name]
    return orphan_dic
a={}
dic=(orphan_codes(a,'UMLS'))

import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor

tupla=()
lista=[]
for i in dic:
    a= i
    b=dic[i][0]
    name=dic[i][1]
    tupla=(a,b,name)
    lista.append(tupla)
    

print(lista)
print(len(lista))

cursor.executemany("insert into orphan_diseases values(%s,%s,%s)",lista)
