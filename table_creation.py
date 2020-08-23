"""DISNET DRUG'S LAYER.

CREATE TABLES, TRACK TABLES AND TRIGGERS.

FILL ENTITY, SOURCE AND CODE_REFERENCE TABLES.
"""


import mysql.connector
from mysql.connector import errorcode
import conection_DISNET_drugslayer
cursor = conection_DISNET_drugslayer.cursor

TABLES={}

TABLES['code_reference'] = (""" CREATE TABLE code_reference (
        resource_id INT PRIMARY KEY,
        resource_name VARCHAR(20) NOT NULL)""")

TABLES['source']=("""CREATE TABLE source (
        source_id int PRIMARY KEY,
        name VARCHAR(25) NOT NULL)""")

TABLES['drug']=("""CREATE TABLE drug (
    drug_id VARCHAR (25) PRIMARY KEY,
    source_id int(11) NOT NULL,
    drug_name varchar(300),
    molecular_type varchar(50),
    chemical_structure varchar(5000),
    inchi_key VARCHAR(250),
        CONSTRAINT fk_drug_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE) """)

TABLES['track_drug']=("""CREATE TABLE track_drug (
    snapshot_date DATE,
    drug_id VARCHAR(25),
    snapshot_action char(1) NOT NULL,
    source_id int(11) NOT NULL,
    drug_name varchar(300),
    molecular_type varchar(50),
    chemical_structure varchar(5000),
    inchi_key VARCHAR(250),
    PRIMARY KEY (snapshot_date,drug_id,snapshot_action),
    KEY drug_id_idx (drug_id)
        )""")

TABLES["drug_BEFORE_INSERT"]=(""" CREATE TRIGGER drug_BEFORE_INSERT BEFORE INSERT ON drug FOR EACH ROW
    INSERT INTO track_drug (snapshot_date,drug_id,snapshot_action,drug_name,molecular_type,chemical_structure,inchi_key,source_id) 
    VALUES (CURDATE(),NEW.drug_id,"I",NEW.drug_name,NEW.molecular_type,NEW.chemical_structure,NEW.inchi_key,NEW.source_id)""")

TABLES["drug_BEFORE_UPDATE"]=(""" CREATE TRIGGER drug_BEFORE_UPDATE BEFORE UPDATE ON drug FOR EACH ROW
    INSERT INTO track_drug (snapshot_date,drug_id,snapshot_action,drug_name,molecular_type,chemical_structure,inchi_key,source_id) 
    VALUES (CURDATE(),NEW.drug_id,"U",NEW.drug_name,NEW.molecular_type,NEW.chemical_structure,NEW.inchi_key,NEW.source_id)""")

TABLES["drug_BEFORE_DELETE"]=(""" CREATE TRIGGER drug_BEFORE_DELETE BEFORE DELETE ON drug FOR EACH ROW
    INSERT INTO track_drug (snapshot_date,drug_id,snapshot_action,drug_name,molecular_type,chemical_structure,inchi_key,source_id) 
    VALUES (CURDATE(),OLD.drug_id,"D",OLD.drug_name,OLD.molecular_type,OLD.chemical_structure,OLD.inchi_key,OLD.source_id)""")

TABLES['ATC_code']=("""CREATE TABLE ATC_code (
    drug_id VARCHAR (25),
    ATC_code_id varchar(12),
    source_id int(11) NOT NULL,
    PRIMARY KEY (drug_id, ATC_code_id),
        CONSTRAINT fk_ATCcode_drugid
        FOREIGN KEY(drug_id)
        references drug(drug_id)
        ON DELETE CASCADE            
        ON UPDATE CASCADE,
            CONSTRAINT fk_ATCcode_sourceid
            FOREIGN KEY(source_id)
            REFERENCES source(source_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE) """)

TABLES['track_ATC_code']=("""CREATE TABLE track_ATC_code (
    snapshot_date DATE,
    drug_id VARCHAR(25),
    ATC_code_id varchar(12),
    snapshot_action char(1) NOT NULL,
    source_id int(11) NOT NULL,
    PRIMARY KEY (snapshot_date,drug_id,ATC_code_id,snapshot_action),
    KEY drugid_ATC_idx (drug_id,ATC_code_id))""")

TABLES["ATC_code_BEFORE_INSERT"]=(""" CREATE TRIGGER ATC_code_BEFORE_INSERT BEFORE INSERT ON ATC_code FOR EACH ROW
    INSERT INTO track_ATC_code (snapshot_date,drug_id,ATC_code_id,snapshot_action,source_id) 
    VALUES (CURDATE(),NEW.drug_id,NEW.ATC_code_id,"I",NEW.source_id)""")

TABLES["ATC_code_BEFORE_UPDATE"]=(""" CREATE TRIGGER ATC_code_BEFORE_UPDATE BEFORE UPDATE ON ATC_code FOR EACH ROW
    INSERT INTO track_ATC_code (snapshot_date,drug_id,ATC_code_id,snapshot_action,source_id) 
    VALUES (CURDATE(),NEW.drug_id,NEW.ATC_code_id,"U",NEW.source_id)""")

TABLES["ATC_code_BEFORE_DELETE"]=(""" CREATE TRIGGER ATC_code_BEFORE_DELETE BEFORE DELETE ON ATC_code FOR EACH ROW
    INSERT INTO track_ATC_code (snapshot_date,drug_id,ATC_code_id,snapshot_action,source_id) 
    VALUES (CURDATE(),OLD.drug_id,OLD.ATC_code_id,"D",OLD.source_id)""")

TABLES['phenotype_effect']=(""" CREATE TABLE phenotype_effect(
    phenotype_id VARCHAR(20),
    source_id int (11),
    phenotype_name VARCHAR(300),
    primary key(phenotype_id,source_id),
        CONSTRAINT fk_phenotypeeffect_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE) """)

TABLES['track_phenotype_effect']=("""CREATE TABLE track_phenotype_effect (
    snapshot_date DATE,
    phenotype_id VARCHAR(25),
    snapshot_action char(1) NOT NULL,
    phenotype_name VARCHAR(300),
    source_id int(11),
    PRIMARY KEY (snapshot_date,phenotype_id,snapshot_action,source_id),
    KEY phenotype_effect_idx (phenotype_id))""")

TABLES["phenotype_effect_BEFORE_INSERT"]=(""" CREATE TRIGGER phenotype_effect_BEFORE_INSERT BEFORE INSERT ON phenotype_effect FOR EACH ROW
    INSERT INTO track_phenotype_effect (snapshot_date,phenotype_id,snapshot_action,phenotype_name,source_id) 
    VALUES (CURDATE(),NEW.phenotype_id,"I",NEW.phenotype_name,NEW.source_id)""")

TABLES["phenotype_effect_BEFORE_UPDATE"]=(""" CREATE TRIGGER phenotype_effect_BEFORE_UPDATE BEFORE UPDATE ON phenotype_effect FOR EACH ROW
    INSERT INTO track_phenotype_effect (snapshot_date,phenotype_id,snapshot_action,phenotype_name,source_id) 
    VALUES (CURDATE(),NEW.phenotype_id,"U",NEW.phenotype_name,NEW.source_id)""")

TABLES["phenotype_effect_BEFORE_DELETE"]=(""" CREATE TRIGGER phenotype_effect_BEFORE_DELETE BEFORE DELETE ON phenotype_effect FOR EACH ROW
    INSERT INTO track_phenotype_effect (snapshot_date,phenotype_id,snapshot_action,phenotype_name,source_id) 
    VALUES (CURDATE(),OLD.phenotype_id,"D",OLD.phenotype_name,OLD.source_id)""")

TABLES['drug_phenotype_effect']=(""" CREATE TABLE drug_phenotype_effect(
    phenotype_id VARCHAR(20),
    drug_id VARCHAR(20),
    source_id int,
    score float,
    phenotype_type VARCHAR(15),
    PRIMARY KEY (phenotype_id,drug_id,source_id),
        CONSTRAINT fk_drugphenotypeeffect_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_drugphenotypeeffect_drugid
            FOREIGN KEY(drug_id)
            references drug(drug_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
                CONSTRAINT fk_drugphenotypeeffect_phenotypeid
                FOREIGN KEY(phenotype_id)
                REFERENCES phenotype_effect(phenotype_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE) """)

TABLES['track_drug_phenotype_effect']=("""CREATE TABLE track_drug_phenotype_effect (
    snapshot_date DATE,
    phenotype_id VARCHAR(25),
    drug_id VARCHAR(20),
    snapshot_action char(1) NOT NULL,
    score float,
    phenotype_type VARCHAR(15),
    source_id int(11),
    PRIMARY KEY (snapshot_date,phenotype_id,drug_id,snapshot_action,source_id),
    KEY drug_phenotype_effect_idx (phenotype_id,drug_id))""")

TABLES["drug_phenotype_effect_BEFORE_INSERT"]=(""" CREATE TRIGGER drug_phenotype_effect_BEFORE_INSERT BEFORE INSERT ON drug_phenotype_effect FOR EACH ROW
    INSERT INTO track_drug_phenotype_effect (snapshot_date,phenotype_id,drug_id,snapshot_action,score,phenotype_type,source_id) 
    VALUES (CURDATE(),NEW.phenotype_id,NEW.drug_id,"I",NEW.score,NEW.phenotype_type,NEW.source_id)""")

TABLES["drug_phenotype_effect_BEFORE_UPDATE"]=(""" CREATE TRIGGER drug_phenotype_effect_BEFORE_UPDATE BEFORE UPDATE ON drug_phenotype_effect FOR EACH ROW
    INSERT INTO track_drug_phenotype_effect (snapshot_date,phenotype_id,drug_id,snapshot_action,score,phenotype_type,source_id) 
    VALUES (CURDATE(),NEW.phenotype_id,NEW.drug_id,"U",NEW.score,NEW.phenotype_type,NEW.source_id)""")

TABLES["drug_phenotype_effect_BEFORE_DELETE"]=(""" CREATE TRIGGER drug_phenotype_effect_BEFORE_DELETE BEFORE DELETE ON drug_phenotype_effect FOR EACH ROW
    INSERT INTO track_drug_phenotype_effect (snapshot_date,phenotype_id,drug_id,snapshot_action,score,phenotype_type,source_id) 
    VALUES (CURDATE(),OLD.phenotype_id,OLD.drug_id,"D",OLD.score,OLD.phenotype_type,OLD.source_id)""")

TABLES['target']=(""" CREATE TABLE target (
    target_id VARCHAR(100),
    source_id int NOT NULL,
    target_name_pref VARCHAR(350),
    target_type VARCHAR(150),
    target_organism VARCHAR(300),
    tax_id INT(11),
    primary key(target_id),
        CONSTRAINT fk_target_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE) """)

TABLES['track_target']=("""CREATE TABLE track_target (
    snapshot_date DATE,
    target_id VARCHAR(100) ,
    snapshot_action char(1) NOT NULL,
    target_name_pref VARCHAR(150),
    target_type VARCHAR(350),
    target_organism VARCHAR(300),
    tax_id INT(11),
    source_id int(11) NOT NULL,
    PRIMARY KEY (snapshot_date,target_id,snapshot_action),
    KEY targetid_idx (target_id))""")

TABLES["target_BEFORE_INSERT"]=(""" CREATE TRIGGER target_BEFORE_INSERT BEFORE INSERT ON target FOR EACH ROW
    INSERT INTO track_target (snapshot_date,target_id,snapshot_action,target_name_pref,target_type,target_organism,tax_id,source_id) 
    VALUES (CURDATE(),NEW.target_id,"I",NEW.target_name_pref,NEW.target_type,NEW.target_organism,NEW.tax_id,NEW.source_id)""")

TABLES["target_BEFORE_UPDATE"]=(""" CREATE TRIGGER target_BEFORE_UPDATE BEFORE UPDATE ON target FOR EACH ROW
    INSERT INTO track_target (snapshot_date,target_id,snapshot_action,target_name_pref,target_type,target_organism,tax_id,source_id) 
    VALUES (CURDATE(),NEW.target_id,"U",NEW.target_name_pref,NEW.target_type,NEW.target_organism,NEW.tax_id,NEW.source_id)""")

TABLES["target_BEFORE_DELETE"]=(""" CREATE TRIGGER target_BEFORE_DELETE BEFORE DELETE ON target FOR EACH ROW
    INSERT INTO track_target (snapshot_date,target_id,snapshot_action,target_name_pref,target_type,target_organism,tax_id,source_id) 
    VALUES (CURDATE(),OLD.target_id,"D",OLD.target_name_pref,OLD.target_type,OLD.target_organism,OLD.tax_id,OLD.source_id)""")

TABLES['drug_target']=(""" CREATE TABLE drug_target (
    target_id VARCHAR(100),
    drug_id VARCHAR (20),
    source_id int NOT NULL,
    target_action_type VARCHAR (150),
    primary key(target_id,drug_id),
        CONSTRAINT fk_drugtarget_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_drugtarget_drugid
            FOREIGN KEY(drug_id)
            REFERENCES drug(drug_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
                CONSTRAINT fk_drugtarget_targetid
                FOREIGN KEY(target_id)
                REFERENCES target(target_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE ) """)

TABLES['track_drug_target']=("""CREATE TABLE track_drug_target (
    snapshot_date DATE,
    target_id VARCHAR(100),
    drug_id VARCHAR (20),
    snapshot_action char(1) NOT NULL,
    target_action_type VARCHAR (150),
    source_id int(11) NOT NULL,
    PRIMARY KEY (snapshot_date,target_id,drug_id,snapshot_action),
    KEY drugtarget_idx (target_id,drug_id))""")

TABLES["drug_target_BEFORE_INSERT"]=(""" CREATE TRIGGER drug_target_BEFORE_INSERT BEFORE INSERT ON drug_target FOR EACH ROW
    INSERT INTO track_drug_target (snapshot_date,target_id,drug_id,snapshot_action,target_action_type,source_id) 
    VALUES (CURDATE(),NEW.target_id,NEW.drug_id,"I",NEW.target_action_type,NEW.source_id)""")

TABLES["drug_target_BEFORE_UPDATE"]=(""" CREATE TRIGGER drug_target_BEFORE_UPDATE BEFORE UPDATE ON drug_target FOR EACH ROW
    INSERT INTO track_drug_target (snapshot_date,target_id,drug_id,snapshot_action,target_action_type,source_id) 
    VALUES (CURDATE(),NEW.target_id,NEW.drug_id,"U",NEW.target_action_type,NEW.source_id)""")

TABLES["drug_target_BEFORE_DELETE"]=(""" CREATE TRIGGER drug_target_BEFORE_DELETE BEFORE DELETE ON drug_target FOR EACH ROW
    INSERT INTO track_drug_target (snapshot_date,target_id,drug_id,snapshot_action,target_action_type,source_id) 
    VALUES (CURDATE(),OLD.target_id,OLD.drug_id,"D",OLD.target_action_type,OLD.source_id)""")

TABLES['disease']=(""" CREATE TABLE disease (
    resource_id int NOT NULL,
    disease_id VARCHAR (25),
    source_id int(11) NOT NULL,
    disease_name VARCHAR (500),
    primary key(disease_id),
        CONSTRAINT fk_disease_resourceid
        FOREIGN KEY(resource_id)
        REFERENCES code_reference(resource_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_disease_sourceid
            FOREIGN KEY(source_id)
            REFERENCES source(source_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE) """)

TABLES['track_disease']=("""CREATE TABLE track_disease (
    snapshot_date DATE,
    disease_id VARCHAR(25),
    snapshot_action char(1) NOT NULL,
    disease_name VARCHAR (500),
    source_id int(11) NOT NULL,
    PRIMARY KEY (snapshot_date,disease_id,snapshot_action),
    KEY disease_idx (disease_id))""")

TABLES["disease_BEFORE_INSERT"]=(""" CREATE TRIGGER disease_BEFORE_INSERT BEFORE INSERT ON disease FOR EACH ROW
    INSERT INTO track_disease (snapshot_date,disease_id,snapshot_action,disease_name,source_id) 
    VALUES (CURDATE(),NEW.disease_id,"I",NEW.disease_name,NEW.source_id)""")

TABLES["disease_BEFORE_UPDATE"]=(""" CREATE TRIGGER disease_BEFORE_UPDATE BEFORE UPDATE ON disease FOR EACH ROW
    INSERT INTO track_drug_target (snapshot_date,disease_id,snapshot_action,disease_name,source_id) 
    VALUES (CURDATE(),NEW.disease_id,"U",NEW.disease_name,NEW.source_id)""")

TABLES["disease_BEFORE_DELETE"]=(""" CREATE TRIGGER disease_BEFORE_DELETE BEFORE DELETE ON disease FOR EACH ROW
    INSERT INTO track_disease (snapshot_date,disease_id,snapshot_action,disease_name,source_id) 
    VALUES (CURDATE(),OLD.disease_id,"D",OLD.disease_name,OLD.source_id)""")

TABLES['drug_disease']=(""" CREATE TABLE drug_disease (
    disease_id varchar(25),
    drug_id varchar (20),
    source_id int NOT NULL ,
    direct_evidence char(1) NOT NULL,
    primary key(disease_id,drug_id),
        CONSTRAINT fk_drugdisease_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_drugdisease_drugid
            FOREIGN KEY (drug_id)
            REFERENCES drug(drug_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
                CONSTRAINT fk_drugdisease_diseaseid
                FOREIGN KEY(disease_id)
                REFERENCES disease(disease_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE ) """)

TABLES['track_drug_disease']=("""CREATE TABLE track_drug_disease (
    snapshot_date DATE,
    disease_id VARCHAR(25),
    drug_id VARCHAR (20),
    snapshot_action char(1) NOT NULL,
    direct_evidence char(1) NOT NULL,
    source_id int(11) NOT NULL,
    PRIMARY KEY (snapshot_date,disease_id,drug_id,snapshot_action),
    KEY disease_id_idx (disease_id,drug_id))""")

TABLES["drug_disease_BEFORE_INSERT"]=(""" CREATE TRIGGER drug_disease_BEFORE_INSERT BEFORE INSERT ON drug_disease FOR EACH ROW
    INSERT INTO track_drug_disease (snapshot_date,disease_id,drug_id,snapshot_action,direct_evidence,source_id) 
    VALUES (CURDATE(),NEW.disease_id,NEW.drug_id,"I",NEW.direct_evidence,NEW.source_id)""")

TABLES["drug_disease_BEFORE_UPDATE"]=(""" CREATE TRIGGER drug_disease_BEFORE_UPDATE BEFORE UPDATE ON drug_disease FOR EACH ROW
    INSERT INTO track_drug_disease (snapshot_date,disease_id,drug_id,snapshot_action,direct_evidence,source_id) 
    VALUES (CURDATE(),NEW.disease_id,NEW.drug_id,"U",NEW.direct_evidence,NEW.source_id)""")

TABLES["drug_disease_BEFORE_DELETE"]=(""" CREATE TRIGGER drug_disease_BEFORE_DELETE BEFORE DELETE ON drug_disease FOR EACH ROW
    INSERT INTO track_drug_disease (snapshot_date,disease_id,drug_id,snapshot_action,direct_evidence,source_id) 
    VALUES (CURDATE(),OLD.disease_id,OLD.drug_id,"D",OLD.direct_evidence,OLD.source_id)""")

TABLES['synonymous']=(""" CREATE TABLE synonymous (
    drug_id VARCHAR(20),
    source_id int (11) NOT NULL,
    synonymous_name VARCHAR(150) NOT NULL,
        CONSTRAINT fk_synonymous_sourceid
        FOREIGN KEY(source_id)
        REFERENCES source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_synonymous_drugid
            FOREIGN KEY (drug_id)
            REFERENCES drug(drug_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE) """)

TABLES['entity'] = (""" CREATE TABLE entity (
        entity_id INT PRIMARY KEY ,
        entity_name VARCHAR(20) NOT NULL)""")

TABLES['code']=(""" CREATE TABLE code (
    code VARCHAR(25) primary key,
    resource_id INT NOT NULL,
    entity_id INT NOT NULL,
    CONSTRAINT fk_code_resourceid
    FOREIGN KEY (resource_id)
    REFERENCES code_reference(resource_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
        CONSTRAINT fk_code_entityid
        FOREIGN KEY (entity_id)
        REFERENCES entity(entity_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE) """)

TABLES['has_code']=(""" CREATE TABLE has_code (
    id_resource_id INT(11) NOT NULL,
    id VARCHAR(25) NOT NULL,
    code VARCHAR(25) NOT NULL,
    resource_id INT NOT NULL,
    entity_id INT NOT NULL,
    CONSTRAINT fk_hascode_resourceid
    FOREIGN KEY (resource_id)
    REFERENCES code_reference(resource_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
        CONSTRAINT fk_hascode_entityid
        FOREIGN KEY (entity_id)
        REFERENCES entity(entity_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
            CONSTRAINT fk_hascode_code
            FOREIGN KEY (code)
            REFERENCES code(code)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
                CONSTRAINT fk_hascode_idresourceid
                FOREIGN KEY (id_resource_id)
                REFERENCES code_reference(resource_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE) """)


# CREATE TABLES
    
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("this table already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# LIST OF SOURCES
sources_list=[(1,"CHEMBL"),(2,"SIDER"),(3,"CTD")]

# LIST OF ENTITIES
entities_list = [(1,"DISEASE"),(2,"DRUG"),(3,"TARGET")]

# LIST OF REFERENCES
references_list=[(75,"MESH"),(95,"DRUGBANK"),(99,"ORPHAN"),(121,"UMLS"),(86,"UNIPROT"),(72,"OMIM"), (97,'CHEMBL')]

#FILL ENTITY, SOURCE AND CODE_REFERENCE TABLES
cursor.executemany("INSERT INTO source VALUES (%s,%s)" ,sources_list)
cursor.executemany("INSERT INTO entity VALUES (%s,%s)" ,entities_list)
cursor.executemany("INSERT INTO code_reference (resource_id, resource_name) VALUES (%s,%s)" ,references_list)
