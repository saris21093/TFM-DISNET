## Author: Sara Jaramillo Cárdenas
## Version: 1.0
"""First time use.

Create tables, insert some important data in the source, entity and code_reference tables

fill all the tables

In order to use these modules it requires to install the following libraries:

install requests
instal chembl-webresource-client

The Authentication module needs a APIKEY.
"""

import conection_DISNET_drugslayer
cursor = conection_DISNET_drugslayer.cursor

# Create tables
import table_creation 


# Fill tables
# It has to be in order

# 1. Drug table: there are info about drug, ATC_code, synonymous, code and has code
import drug_table

# 2. phenotype effect and drug- phenotype effect when phenotype type is SIDE EFFECT
import side_effect_table

#3. phenotype effect when phenotype type is INDICATION and also code and has code
import indication_table

#4. drug - phenotype effect when phenotype type is INDICATION
import drug_indication_table

#5 target table and also code and has code
import target_table

#6 drug - target 
import drug_target_table

#7 disease and drug - disease
import disease_table

#8. Get orphan code and umls code from disease table
import cross_reference