
# coding: utf-8

# # Make diff between SE and FC

# We just make sure that jupyter will display all output

# In[2]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# necessary imports

# In[3]:


import pandas as pd
import numpy as np


# read the data

# In[4]:


# seDef =  pd.DataFrame({'seName': ['SE1', 'SE2', 'FAILOVER'], 'basePath':['/base1', '/base2', '/base1/failover']})
# seDef.set_index('seName', inplace = True)
seDef = pd.read_csv('seDef.csv', names = ['seName', 'basePath'], delimiter = ';', index_col = 'seName')

seDef
fc_dump = pd.read_csv('fc.csv', names = ['seName', 'lfn','cks','size'], delimiter = ';')
fc_dump['version'] = 'fc_dump'
se = pd.read_csv('se.csv', names = ['pfn','cks','size'], delimiter = ';', index_col = 'pfn')
se['version'] = 'se'


# Join basePath and LFN



fc_dump['pfn'] = fc_dump.apply( lambda x: seDef.loc[x['seName']] + x['lfn'],axis=1)
fc_dump.set_index('pfn', inplace = True)



# Join all the data together.
# `ignore_index` is used to keep the LFN as indeces


# We put in union_fc the content from both catalog
#union_fc = pd.concat([fc_dump, new_fc],ignore_index=False)

# Files in any FC, but unique in the list
#any_fc = union_fc[~union_fc.index.duplicated(keep = 'first')]
#any_fc.loc[:, ('version',)] = 'any_fc'

# Files in both. We assume the checksum didn't change
#both_fc = union_fc[union_fc.index.duplicated(keep = 'first')]
#both_fc.loc[:, ('version',)] = 'both_fc'



# ## Dark data

# The dark data are files that are in SE and in none of the FC. 

# In[8]:


darkData = se.index.difference(fc_dump.index)
# We make a dataframe from the index just because it is better looking !
print "Dark data"
ddFrame =  pd.DataFrame(index = darkData)
ddFrame.to_csv('dataToBeRemoved.csv')



# ## Lost data

# For lost data, we jsut check the files that are in both catalogs, but not in the SE


lostData=  fc_dump.index.difference(se.index)
print "Lost data"
lostDataFrame = fc_dump.loc[lostData, :]
lostDataFrame.to_csv('dataNotCopiedToRAL.csv')
