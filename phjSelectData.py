import numpy as np
import pandas as pd



def phjPrintIndexHeading(i):
	print('\n' + '*'*8 + '*'*len(str(i)))
	print('* i = ' + str(i) + ' *')
	print('*'*8 + '*'*len(str(i)))
	
	return



def phjParameterCheck(phjCasesDF,
					  phjPotentialControlsDF,
					  phjUniqueIdentifierVarName,
					  phjMatchingVariablesList,
					  phjControlsPerCaseInt):
	# Get list of column names from cases and controls dataframes
	phjCasesDFColumnNamesList = phjCasesDF.columns.values.tolist()
	phjPotentialControlsDFColumnNamesList = phjPotentialControlsDF.columns.values.tolist()
	
	
	# Start off by assuming that the parameters are all OK and set the phjTempCheckVar
	# variable to True. Then make checks and, if the check fails, set the phjTempCheckVar
	# to False.
	phjTempCheckVar = True
	
	# Check that phjMatchingVariablesList is a list and not a string
	if not isinstance(phjMatchingVariablesList,list):
		print('The phjMatchingVariablesList parameter passed to this function was not a list.')
		phjTempCheckVar = False
		
	# Check that phjControlsPerCase is an int
	if not isinstance(phjControlsPerCaseInt,int):
		print('The phjControlsPerCase parameter passed to this function was not an integer.')
		phjTempCheckVar = False
	
	# Check that 'group' and 'case' columns do not already exist in dataframe.
	# This function will return a database that includes columns entitled 'group'
	# and 'case'. Therefore, it is important to check that these names are
	# not included in the original dataframe columns. In fact, it would only be
	# necessary to check that 'group' and 'case' are not in the phjUniqueIdentifier
	# or the phjMatchingVariablesList since these are the only columns that are included
	# in the returned dataframe. However, columns named 'group' or 'case' in the original
	# database would lead to confusion. Therefore, recommend that columns 'group' and
	# 'case' should be renamed before running this function.
	if (('group' in phjCasesDFColumnNamesList) | ('case' in phjCasesDFColumnNamesList) | 
		('group' in phjPotentialControlsDFColumnNamesList) | ('case' in phjPotentialControlsDFColumnNamesList)):
		print("This function aims to return a dataframe with added columns called 'group' and 'case'. However, columns with these names already exist. Please rename these columns and re-run this function.")
		phjTempCheckVar = False
		
	# Check that phjUniqueIdentifier is contained within both phjCasesDF and
	# phjPotentialControlsDF. If not, then return None.
	if ((phjUniqueIdentifierVarName not in phjCasesDFColumnNamesList) | (phjUniqueIdentifierVarName not in phjPotentialControlsDFColumnNamesList)):
		print('The variable ', phjUniqueIdentifierVarName, ' is not contained in the cases and/or potential controls dataframes.')
		phjTempCheckVar = False
		
	# Check that phjUniqueIdentifierVarName columns in both case and controls dataframe
	# contains unique values.
	# Firstly, join unique identifier columns into a single series and then compare
	# total length with length of unique series.
	phjTempSeries = pd.concat([phjCasesDF[phjUniqueIdentifierVarName],phjPotentialControlsDF[phjUniqueIdentifierVarName]],axis = 0)
	
	if (phjTempSeries.size > phjTempSeries.unique().size):
		print("The unique identifier variable does not contain unique values.")
		phjTempCheckVar = False
	
	# Check that the variable names in the phjMatchingVariablesList are all contained
	# within both phjCasesDF and phjPotentialControlDF.
	for phjTempListItem in phjMatchingVariablesList:
		if ((phjTempListItem not in phjCasesDFColumnNamesList) | (phjTempListItem not in phjPotentialControlsDFColumnNamesList)):
			print('The variable ', phjTempListItem, 'is not in the cases and/or potential controls dataframes.')
			phjTempCheckVar = False
			
	return phjTempCheckVar



def phjAddRecords(phjTempCaseControlDF,
				  phjUniqueIdentifierVarName,
				  phjUniqueIdentifierValue,
				  phjMatchingVariablesList,
				  phjMatchingVariablesValues,
				  phjTempRowCounter,
				  phjCaseVarName = 'case',
				  phjCaseValue = None,
				  phjGroupVarName = 'group',
				  phjGroupValue = None,
				  phjPrintResults = False):
	
	if phjPrintResults == True:
		print('\nPrint passed parameters')
		print('-----------------------')
		print('phjUniqueIdentifierVarName',phjUniqueIdentifierVarName)
		print('phjUniqueIdentifierValue',phjUniqueIdentifierValue)
		print('phjMatchingVariablesList',phjMatchingVariablesList)
		print('phjMatchingVariablesValues',phjMatchingVariablesValues)
		print('phjTempRowCounter',phjTempRowCounter)
		print('phjCaseVarName',phjCaseVarName)
		print('phjCaseValue',phjCaseValue)
		print('phjGroupVarName',phjGroupVarName)
		print('phjGroupValue',phjGroupValue)
	
	phjTempCaseControlDF.ix[phjTempRowCounter,phjUniqueIdentifierVarName] = phjUniqueIdentifierValue
	phjTempCaseControlDF.ix[phjTempRowCounter,phjGroupVarName] = phjGroupValue
	phjTempCaseControlDF.ix[phjTempRowCounter,phjCaseVarName] = phjCaseValue
	
	for phjTempVar in phjMatchingVariablesList:
		phjTempCaseControlDF.ix[phjTempRowCounter,phjTempVar] = phjMatchingVariablesValues[phjTempVar].tolist()
	
	return phjTempCaseControlDF



def phjSelectCaseControlDataset(phjCasesDF,
								phjPotentialControlsDF,
								phjUniqueIdentifierVarName,
								phjMatchingVariablesList = None,
								phjControlsPerCaseInt=1,
								phjPrintResults = False):
	
	# Print summary of parameters passed to function
	if phjPrintResults == True:
		with pd.option_context('display.max_rows', 6, 'display.max_columns', 6):
			print('\nCASES\n=====')
			print(phjCasesDF)
			print('\nPOTENTIAL CONTROLS\n==================\n')
			print(phjPotentialControlsDF)
		print('Unique identifier variable = ', phjUniqueIdentifierVarName)
		print('Number of controls to be selected per case = ',phjControlsPerCaseInt)
		print('Variables to match = ',phjMatchingVariablesList)
		
		print('Number of potential controls = ',len(phjPotentialControlsDF))
		
	# Checks on parameters passed to functions
	# ========================================
	# Run function phjParameterCheck() to make sure parameters passed to function make
	# sense. If the function returns True then go ahead and create case-control dataset.
	# If returns False then return None.
	phjParameterCheckResult = phjParameterCheck(phjCasesDF = phjCasesDF,
												phjPotentialControlsDF = phjPotentialControlsDF,
												phjUniqueIdentifierVarName = phjUniqueIdentifierVarName,
												phjMatchingVariablesList = phjMatchingVariablesList,
												phjControlsPerCaseInt = phjControlsPerCaseInt)
	
	# The phjParameterCheck() function returns True if all parameters are OK and False
	# if there are any errors. If all the parameters are OK (and phjParameterCheckResult
	# is True) then go ahead and create the case-control dataset. Otherwise, don't.
	if phjParameterCheckResult:
		
		# Algorithm outline
		# =================
		# 1. Create an empty dataframe in which selected cases and controls will be stored.
		# 2. Step through each case in the phjCasesDF dataframe, one at a time.
		# 3. Get data from matched variables for the case and store in a dict
		# 4. Create a mask for the controls dataframe to select all controls that match the cases in the matched variables
		# 5. Apply mask to controls dataframe and count number of potential matches
		# 6. Add cases and controls to dataframe
		# 7. Remove added control records from potential controls database so single case cannot be selected more than once
		# 8. Return dataframe containing list of cases and controls. This dataframe only
		#    contains columns with unique identifier, case and group id. It will,
		#    therefore need to be merged with the full database to get all other columns.
	
		# 1. Create empty dataframe
		# -------------------------
		# Create empty dataframe of known dimensions to hold cases and control data
		# Required length calculated as number of cases plus number of controls
		# (i.e. cases x number of matched controls). The variable list is the list
		# of matched variables plus a column to signify group and a column to signify
		# whether a case or a control and the unique identifier variable.
		phjTempCaseControlDFLength		= len(phjCasesDF) + (len(phjCasesDF) * phjControlsPerCaseInt)
		phjTempCaseControlDFColumnsList	= [phjUniqueIdentifierVarName] + ['group','case'] + phjMatchingVariablesList
		phjTempCaseControlDF			= pd.DataFrame( index = range(0,phjTempCaseControlDFLength),
														columns = phjTempCaseControlDFColumnsList)
		
		if phjPrintResults == True:
			print('Temp case-control dataframe column list = ', phjTempCaseControlDFColumnsList)
			
		# Set counter to keep track of which row to add data to.
		phjTempRowCounter = 0
		
		# 2. Step through each case in the phjCasesDF dataframe, one at a time
		# --------------------------------------------------------------------
		for i in phjCasesDF.index:
			if phjPrintResults == True:
				# Print a heading for the ith case
				phjPrintIndexHeading(i)
				
			if phjPrintResults == True:
				print('\nLength of phjPotentialControls = ',phjPotentialControlsDF.index.size)
			
			# Reset some variables
			# --------------------
			# Reset number of available controls to np.nan
			phjTempNumberAvailableControls = np.nan
			
			# Clear phjTempDict (which will hold dict of case data.
			# N.B. using myDict = {} will create a new instance of myDict but other
			# references will still point to the old version of myDict with its original
			# data. (See http://stackoverflow.com/questions/369898/difference-between-dict-clear-and-assigning-in-python)
			# In contrast, myDict.clear() will clear the data.
			# But Python doesn't have a way to check if a variable has been defined
			# since it expects all variables to be defined before use. But can try to
			# access a variable and, if it fails, an exception will be raised.
			try:
				phjTempDict.clear()
			except NameError:
				phjTempDict = {}
				
				
			# 3. Get data from matched variables for the case
			# -----------------------------------------------
			# Create a dict for the data held in the matching variables in each case
			phjTempDict = phjCasesDF.ix[i,phjMatchingVariablesList].to_dict()
			
			# The above dict is of the form: {'var1': 'a', 'var2': 3}. However, this needs
			# to be passed to a df.isin() function and, as such, needs to be of the form:
			# {'var1': ['a'], 'var2': [3]}. Make this conversion using a dictionary
			# comprehension (as suggested by zondo 12 Mar 2016 - see:
			# http://stackoverflow.com/questions/35961614/converting-a-dict-to-a-dict-of-lists-in-python ).
			phjTempDict = {key: [value] for key, value in phjTempDict.items()}
			
			if phjPrintResults == True:
				print('\nphjTempDict =')
				print(phjTempDict)
				
				
			# 4. Create a mask for the controls dataframe
			# -------------------------------------------
			# Create a mask for the controls dataframe to select all controls that match the cases on the matched variables
			# phjTempMask = pd.DataFrame([phjPotentialControlsDF[key] == val for key, val in phjTempDict.items()]).T.all(axis=1)
			phjTempMask = phjPotentialControlsDF[phjMatchingVariablesList].isin(phjTempDict).all(axis=1)
			
			# 5. Apply mask to controls DF and count length
			# ---------------------------------------------
			phjTempMatchingControlsDF = phjPotentialControlsDF[phjTempMask]
		
			phjTempNumberAvailableControls = len(phjTempMatchingControlsDF)
		
			if phjPrintResults == True:
				print('\nNumber of available controls = ',phjTempNumberAvailableControls)
				with pd.option_context('display.max_rows', 6, 'display.max_columns', 6):
					print('\nMatching controls')
					print(phjTempMatchingControlsDF)
					
					
			# 6. Add cases and controls to dataframe
			# --------------------------------------
			# Python doesn't have switch-case structure. Use if...elif...else structure instead.
			# If there are no suitable controls then the case is not included
			# If the number of suitable controls is less than required then add all available
			# If there are more than request number of controls then select a random sample
			
			# i. If no controls then dump case (i.e. don't add it to the dataframe)
			# - - - - - - - - - - - - - - - - 
			if phjTempNumberAvailableControls == 0:
				if phjPrintResults == True:
					# Presumably don't include case in the final dataframe
					print('Available controls = 0. Case not included in final dataframe.')
				else:
					pass
				
			# ii. If less than (or equal to) requested number of controls then add all controls to dataframe
			# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
			elif (phjTempNumberAvailableControls > 0) & (phjTempNumberAvailableControls <= phjControlsPerCaseInt):
				if phjPrintResults == True:
					print('\nAvailable controls = ', phjTempNumberAvailableControls, '. Requested number of controls = ',phjControlsPerCaseInt, '.')
					
				# Add case to dataframe
				# - - - - - - - - - - -
				phjTempCaseControlDF = phjAddRecords(phjTempCaseControlDF,
													 phjUniqueIdentifierVarName = phjUniqueIdentifierVarName,
													 phjUniqueIdentifierValue = phjCasesDF.ix[i,phjUniqueIdentifierVarName],
													 phjMatchingVariablesList = phjMatchingVariablesList,
													 phjMatchingVariablesValues = phjCasesDF.ix[[i],phjMatchingVariablesList],
													 phjTempRowCounter = [phjTempRowCounter],
													 phjCaseVarName = 'case',
													 phjCaseValue = [1],
													 phjGroupVarName = 'group',
													 phjGroupValue = [i],
													 phjPrintResults = phjPrintResults)
				
				if phjPrintResults == True:
					print('\nCase\n----')
					print(phjTempCaseControlDF.ix[phjTempRowCounter,:])
				
				# Increment row counter by 1
				phjTempRowCounter = phjTempRowCounter + 1
				
				# Add all available controls to the final dataframe
				# - - - - - - - - - - - - - - - - - - - - - - - - -
				if phjPrintResults == True:
					print('\nControls\n--------')
					print('All matching controls')
					with pd.option_context('display.max_rows', 6, 'display.max_columns', 6):
						print(phjTempMatchingControlsDF)
				
				phjTempCaseControlDF = phjAddRecords(phjTempCaseControlDF,
													 phjUniqueIdentifierVarName = phjUniqueIdentifierVarName,
													 phjUniqueIdentifierValue = phjTempSampleMatchingControlsDF[phjUniqueIdentifierVarName].tolist(),
													 phjMatchingVariablesList = phjMatchingVariablesList,
													 phjMatchingVariablesValues = phjCasesDF.ix[[i],phjMatchingVariablesList],
													 phjTempRowCounter = range(phjTempRowCounter, (phjTempRowCounter + phjTempNumberAvailableControls)),
													 phjCaseVarName = 'case',
													 phjCaseValue = [0]*phjTempNumberAvailableControls,
													 phjGroupVarName = 'group',
													 phjGroupValue = [i]*phjTempNumberAvailableControls,
													 phjPrintResults = phjPrintResults)
				
				if phjPrintResults == True:
					print('\nControls in dataframe')
					print(phjTempCaseControlDF.loc[phjTempRowRange])
				
				# 7. Delete selected controls from dataframe!!!
				# ------------------------------------------
				phjPotentialControlsDF = phjPotentialControlsDF[~phjPotentialControlsDF[phjUniqueIdentifierVarName].isin(tempeMatchingControlsDF[phjUniqueIdentifierVarName].tolist())]
				
				# Increment row counter by number of available controls
				phjTempRowCounter = phjTempRowCounter + phjTempNumberAvailableControls
				
			
			# iii. If more than requested number of controls then add selection of controls to dataframe
			# ------------------------------------------------------------------------------------------
			elif (phjTempNumberAvailableControls > phjControlsPerCaseInt):
				if phjPrintResults == True:
					print('\nAvailable controls = ', phjTempNumberAvailableControls, '. Requested number of controls = ',phjControlsPerCaseInt, '.')
				
				# Add case to dataframe
				# - - - - - - - - - - -
				phjTempCaseControlDF = phjAddRecords(phjTempCaseControlDF,
													 phjUniqueIdentifierVarName = phjUniqueIdentifierVarName,
													 phjUniqueIdentifierValue = phjCasesDF.ix[i,phjUniqueIdentifierVarName],
													 phjMatchingVariablesList = phjMatchingVariablesList,
													 phjMatchingVariablesValues = phjCasesDF.ix[[i],phjMatchingVariablesList],
													 phjTempRowCounter = [phjTempRowCounter],
													 phjCaseVarName = 'case',
													 phjCaseValue = [1],
													 phjGroupVarName = 'group',
													 phjGroupValue = [i],
													 phjPrintResults = phjPrintResults)
				
				if phjPrintResults == True:
					print('\nCase\n----')
					print(phjTempCaseControlDF.ix[phjTempRowCounter,:])
				
				# Increment row counter by 1
				phjTempRowCounter = phjTempRowCounter + 1
			
				# Add random selection of controls to dataframe
				# - - - - - - - - - - - - - - - - - - - - - - -
				phjTempSampleMatchingControlsDF = phjTempMatchingControlsDF.sample( n = phjControlsPerCaseInt,
																					replace = False,
																					axis = 0)
				
				if phjPrintResults == True:
					print('\nControls\n--------')
					print('All matching controls')
					with pd.option_context('display.max_rows', 6, 'display.max_columns', 6):
						print(phjTempSampleMatchingControlsDF)
				
				phjTempRowRange = range(phjTempRowCounter, (phjTempRowCounter + phjControlsPerCaseInt))
			
				phjTempCaseControlDF = phjAddRecords(phjTempCaseControlDF,
													 phjUniqueIdentifierVarName = phjUniqueIdentifierVarName,
													 phjUniqueIdentifierValue = phjTempSampleMatchingControlsDF[phjUniqueIdentifierVarName].tolist(),
													 phjMatchingVariablesList = phjMatchingVariablesList,
													 phjMatchingVariablesValues = phjCasesDF.ix[[i],phjMatchingVariablesList],
													 phjTempRowCounter = range(phjTempRowCounter, (phjTempRowCounter + phjControlsPerCaseInt)),
													 phjCaseVarName = 'case',
													 phjCaseValue = [0]*phjControlsPerCaseInt,
													 phjGroupVarName = 'group',
													 phjGroupValue = [i]*phjControlsPerCaseInt,
													 phjPrintResults = phjPrintResults)
				
				if phjPrintResults == True:
					print('\nControls in dataframe')
					print(phjTempCaseControlDF.loc[phjTempRowRange])
					
				# 7. Delete selected controls from dataframe!!!
				# ------------------------------------------
				phjPotentialControlsDF = phjPotentialControlsDF[~phjPotentialControlsDF[phjUniqueIdentifierVarName].isin(phjTempSampleMatchingControlsDF[phjUniqueIdentifierVarName].tolist())]
				
				# Increment row counter by number of controls requested per case
				phjTempRowCounter = phjTempRowCounter + phjControlsPerCaseInt
				
				
			# If none of the above criteria is met
			# ------------------------------------
			else:
				if phjPrintResults == True:
					# Report that something went wrong
					print('Something went wrong')
				else:
					pass
					
					
		return phjTempCaseControlDF
		
	# If phjParameterCheck() function is False then return None from this function
	else:
		return None