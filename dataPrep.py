#!/usr/bin/python

__author__ = 'lar'
_version_ = '0.1.0'


import string
import sys
import numpy as np
import pandas as pd
import requests
import argparse
import csv
import re
from datetime import *


def specify(specifyName):

    if args.include != None:
        file_includes = args.include
    else:
        file_includes = specifyName

    try:
        fInSpecify = open(file_includes, "r")
        fInSpecify.close()
    except IOError as e:
        print "File not found: .txt file '{0}' does not appear to exist.".format(specifyName)
        sys.exit(1)

    specList = []
    with open(file_includes, "r") as fInSpecify:
        for line in fInSpecify:
            specList.append(line.strip())
    fInSpecify.close()

    return specList


def CityInCounty(city, state):                          # ToDo: Horribly inefficient now.  Pickle / SQL / dynamic cache
    urlEncodedCity = city.replace(" ", '%20')
    r = requests.get('http://api.sba.gov/geodata/all_links_for_city_of/' + urlEncodedCity + '/' + state + '.json')

    response = r.json()

    if response == []:
#        print 'N/A'                            # Debug / Logging
        return 'N/A'
    else:
#        print response[0][u'county_name']      # Debug / Logging
        return response[0][u'county_name']



if __name__ == "__main__":

    k_Number_Days_Monthly = 30.4368

    parser = argparse.ArgumentParser(description='Usage: prepare raw data into standard fields and types.')
    parser.add_argument('keyfile', help='a specially-formatted .csv file that directs the conversion process')
    parser.add_argument('-V', '--version', help='show the version number and exit', action='store_true')
    parser.add_argument('-d','--datafile', help='the .csv datafile that will be processed')
    parser.add_argument('-u','--update', help='date when the samples were updated (YYYY-MM-DD)')
    parser.add_argument('-o','--output', help='the "root" output filename')
    parser.add_argument('-S','--summary', help='statistical summary of the filtered datafile', action='store_true')
    parser.add_argument('-p','--percentage', help='the percentage of the data that will be in the training set')
    parser.add_argument('-s','--samples', help='limit the output file to no more than the number of samples')
    parser.add_argument('-e','--entriesparsed', help='output an update when the requested lines have been parsed from input', default=10000)
    parser.add_argument('-i','--include', help='"anti-filter" - include only the samples that are in this file')

    args = parser.parse_args()


    if args.version:
        print
        print sys.argv[0] + ':  Version ' + _version_
        sys.exit(0)



    try:
        fInKeyCSV = open(args.keyfile, "r")
        fInKeyCSV.close()

    except IOError as e:
        print "File not found: .csv file '{0}' does not appear to exist.".format(args.keyfile)
        sys.exit(1)

    fInKeyCSV = open(args.keyfile, "rU",)                         # 'rU':  universal-newline
    keyReader = csv.reader(fInKeyCSV, dialect='excel')

    globalDict = {}
    filterList = []
    fieldList  = []

    inGlobals = False
    inFilters = False
    inFields = False

    for row in keyReader:                                       # each row generates a list, row[0] is the first string
        if row[0] == '':                                        # Blank line?
            continue
        if row[0].startswith('#'):                              # Comment line?
            continue
        elif row[0].startswith('GLOBALS'):                      # GLOBALS section?
            inGlobals = True
            continue
        elif row[0].startswith('FILTERS'):                      # FILTERS section?
            inFilters = True
            inGlobals = False
            continue
        elif row[0].startswith('FIELDS'):                       # FIELDS section?
            inFields = True
            inFilters = False
            continue
        else:
            if inGlobals:
                globalDict[row[0]] = row[1]                     # Field 0: global name, Field 1: value
            elif inFilters:
                filterList.append(row[0:3])                     # Field 0: filtered value, Field 1: filter, Field 2: value
            elif inFields:
                fieldList.append(row)                           # All fields (for now...)


    print
    for globalKey in globalDict:
        print globalKey + ', ' + globalDict[globalKey]
    print
    for filterRow in filterList:
        print filterRow
    print
    for fieldRow in fieldList:
        print fieldRow
    print



    gLDDRevMajor = int(globalDict.get('g_LDD_Rev_Major', '0'))          # ToDo:  "REAL' versioning...
    gLDDRevMinor = int(globalDict.get('g_LDD_Rev_Minor', '0'))
    if (gLDDRevMajor == 0) and (gLDDRevMinor < 3):
        print
        print "Incompatible Key File. Must have 0.3 or higher."
        sys.exit(1)



    dataCSV = ''                                            # todo:  None or ''
    if args.datafile != '':
        dataCSV = args.datafile
    elif globalDict.get('g_Datafile') != '':
        dataCSV = globalDict.get('g_Datafile')
    else:
        print
        print "No data file - please specify on the command line or in the key file."
        sys.exit(1)

    try:
        fInDataCSV = open(dataCSV, "r")
        fInDataCSV.close()

    except IOError as e:
        print "File not found: .csv file '{0}' does not appear to exist.".format(dataCSV)
        sys.exit(1)



    updateStr = None
    if args.update != None:
        updateStr = args.update
    elif globalDict.get('g_Update') != None:
        updateStr = globalDict.get('g_Update')
    if updateStr != None:
        try:
            updateDateTime = datetime.strptime(updateStr, '%Y-%m-%d').date()
        except ValueError as e:
            print "Update date must have the format YYYY-MM-DD."
            sys.exit(1)



    outFileRoot = None
    if args.output != None:
        outFileRoot = args.output
    elif globalDict.get('g_Output') != None:
        outFileRoot = globalDict.get('g_Output')
    else:
        outFileRoot = datetime.now().strftime("%y%b%d-%Hh%Mm%Ss") + ' '

    try:
        fOutTrain = open(outFileRoot + "Train_.csv", "w")           # todo:  master file for analysis with pandas, etc
        fOutTest = open(outFileRoot + "Test_.csv", "w")

    except IOError as e:
        print "Could not open training or test files for output. "
        sys.exit(1)

    g_ML_Format = globalDict.get('g_ML_Format', 'csv')
    try:
        fOutTrain_ML = open(outFileRoot + "Train_ML_." + g_ML_Format, "w")
        fOutTest_ML = open(outFileRoot + "Test_ML_." + g_ML_Format, "w")

    except IOError as e:
        print "Could not open machine learning training or test files for output. "
        sys.exit(1)



    percentTrainingSamples = None
    if args.percentage != None:
        percentTrainingSamples = float(args.percentage)
    elif globalDict.get('g_Percent_Train') != None:
        percentTrainingSamples = float(globalDict.get('g_Percent_Train'))
    else:
        percentTrainingSamples = 100.0

    trainingFraction = percentTrainingSamples / 100.0



    maxSamples = None
    if args.samples != None:
        maxSamples = int(args.samples)
    elif globalDict.get('g_Max_Samples') != None:
        maxSamples = int(globalDict.get('g_Max_Samples'))
    else:
        maxSamples = -1



    line_ctr = 0
    crit_ctr = 0
    incrementingFraction = 0.0
    intFractionPart = 1
    specifyFilter = ''                                                  # ONLY ONE "specify" for filters section...

    fiHeaderRow = True
    fInDataCSV = open(dataCSV, "rU",)                                       # 'rU':  universal-newline
    fiReader = csv.reader(fInDataCSV, dialect='excel')

    keyColumnDict = {}                                              # key: key fields, values: index into field list

    fieldCount = 0
    for fieldRow in fieldList[0]:                                   # fields in key defined in 1st row/list
        keyColumnDict[fieldList[0][fieldCount]] = fieldCount
        fieldCount +=1

    key_Abbr_List = []
    column_Header_List = []
    ldd_Visible_Field_List = []
    ldd_NonVisible_Index_List = []
    ml_Include_List = []
    ml_Type_List = []

    for fiRow in fiReader:

        if fiHeaderRow:
            fiHeaderRow = False
            fiHeader = fiRow

            ldd_nonVisibleIndexCount = 0
            for fieldRow in fieldList[1:]:                      # Build corresponding ldd_Field <> FI field number list

                ldd_Field = fieldRow[keyColumnDict['ldd_Field']]
                if fieldRow[keyColumnDict['ldd_Derived']] != "TRUE":
                    fi_Column = fiHeader.index(fieldRow[keyColumnDict['fi_Field']])
                else:
                    fi_Column = -1                                                          # Derived field convention
                key_Abbr_List.append((ldd_Field, fi_Column))
                column_Header_List.append(ldd_Field)

#            header = ",".join(column_Header_List)

                if fieldRow[keyColumnDict['ldd_Visible_and_Included']] != "FALSE":
                    ldd_Visible_Field_List.append(ldd_Field)
                else:
                    ldd_NonVisible_Index_List.insert(0, ldd_nonVisibleIndexCount)
                ldd_nonVisibleIndexCount += 1

            header = ",".join(ldd_Visible_Field_List)


            print                                                                           # debug
            print 'Complete Field Header'                                                   # debug
            print header                                                                    # Debug

            fOutTrain.write("{0}\n".format(header))
            fOutTest.write("{0}\n".format(header))


            for fieldRow in fieldList[1:]:                      # Build a subset of ldd_Field <> ML field list

                ldd_Field = fieldRow[keyColumnDict['ldd_Field']]
                if (fieldRow[keyColumnDict['ml_Include']] == "TRUE") \
                    or (fieldRow[keyColumnDict['ml_Include']] == "TARGET"):
                    ml_Include_List.append(ldd_Field)
                    ml_Type_List.append(fieldRow[keyColumnDict['ml_Type_ARFF']])

            mlHeader = ",".join(ml_Include_List)

            print                                                               # debug
            print 'Machine Learning Header:'                                    # debug
            print mlHeader                                                      # debug
            print                                                               # debug

            if g_ML_Format == 'arff':

                fOutTrain_ML.write("% (c) 2014 Lendalytics, Inc.  All rights reserved.  \n")
                fOutTest_ML.write("% (c) 2014 Lendalytics, Inc.  All rights reserved.  \n")

                fOutTrain_ML.write("\n")
                fOutTest_ML.write("\n")

                fOutTrain_ML.write("@RELATION {0}\n".format(globalDict.get('g_Relation', '*UNSPECIFIED*')))
                fOutTest_ML.write("@RELATION {0}\n".format(globalDict.get('g_Relation', '*UNSPECIFIED*')))

                fOutTrain_ML.write("\n")
                fOutTest_ML.write("\n")

                for ml_Includes, ml_Types in zip(ml_Include_List, ml_Type_List):
                    fOutTrain_ML.write("@ATTRIBUTE {0:32}  {1}\n".format(ml_Includes, ml_Types))
                    fOutTest_ML.write("@ATTRIBUTE {0:32}  {1}\n".format(ml_Includes, ml_Types))

                fOutTrain_ML.write("\n")
                fOutTest_ML.write("\n")

                fOutTrain_ML.write("@Data\n".format(mlHeader))
                fOutTest_ML.write("@Data\n".format(mlHeader))

            elif (g_ML_Format == 'csv'):
                fOutTrain_ML.write("{0}\n".format(mlHeader))
                fOutTest_ML.write("{0}\n".format(mlHeader))

            continue

        else:

            # process FI data a loan at a time; formatted as per key
            line_ctr += 1                                                           # todo command line
            if line_ctr % int(args.entriesparsed) == 0:
                print "...{0} entries parsed...".format(line_ctr)

            column_Vals_List = []
            ml_Vals_List = []

            derived = False                                                                 # todo:  necessary?

            for tuple in key_Abbr_List:
                ldd_Field, fi_Column = tuple
                if fi_Column != -1:
                    column_Vals_List.append(fiRow[fi_Column])
                else:
                    column_Vals_List.append("_DERIVED_")
                    derived = True

#            outline = ",".join(column_Vals_List)                                            # debug
#            print outline                                                                   # debug

            # type conversion
            column = -1
            for fieldRow in fieldList[1:]:
                column += 1

#                ldd_Field = fieldRow[keyColumnDict['ldd_Field']]                          # debug

                ldd_Type_Conversion = fieldRow[keyColumnDict['ldd_Type_Conversion']]
                ldd_Type_Format = fieldRow[keyColumnDict['ldd_Type_Format']]
                if (ldd_Type_Conversion == "NONE") or (ldd_Type_Conversion == ""):
                    pass

                elif ldd_Type_Conversion == "REGEX":
                    searchResult = re.search(ldd_Type_Format, column_Vals_List[column])
                    if searchResult:
                        column_Vals_List[column] = searchResult.group(0)

                elif ldd_Type_Conversion == "DATE":
                    if column_Vals_List[column] != '':
                        datetimeResult = datetime.strptime(column_Vals_List[column], ldd_Type_Format)
                        column_Vals_List[column] = datetimeResult.date().isoformat()

                elif ldd_Type_Conversion == "EVAL":
                    if column_Vals_List[column] != '':
                        expression = ldd_Type_Format.replace('{}', column_Vals_List[column])
                        column_Vals_List[column] = str(eval(expression))


                elif ldd_Type_Conversion == "EXEC":
                    exec_List = []
                    if column_Vals_List[column] != '':
                        exec_Line = str(ldd_Type_Format.replace('{}', column_Vals_List[column]))
                        _ER_ = ''
                        exec_List = exec_Line.split('#')
                        exec_List = '\n'.join(exec_List)
                        exec_List = exec_List.replace('\\t', '    ')
                        exec(exec_List)
                        column_Vals_List[column] = str(_ER_)


#            print                                                                           # debug
#            outline = ",".join(column_Vals_List)                                            # debug
#            print outline                                                                   # debug


            # Derived Fields
            while derived is True:

                derived = False                                             # todo:  Necessary?
                column = -1
                for fieldRow in fieldList[1:]:
                    column += 1

                    ldd_Derived = fieldRow[keyColumnDict['ldd_Derived']]
                    if ldd_Derived == "TRUE":
                        ldd_Type_Conversion = fieldRow[keyColumnDict['ldd_Type_Conversion']]
                        ldd_Type_Format = fieldRow[keyColumnDict['ldd_Type_Format']]

                        if column_Vals_List[column] == '_DERIVED_':

                            if ldd_Type_Conversion == "D.EVAL":
                                unpopulated = False
                                eval_List = ldd_Type_Format.split('|')
                                for ldd_Fields in eval_List[1:]:
                                    fieldNum = column_Header_List.index(ldd_Fields)
                                    val = (column_Vals_List[fieldNum])
                                    if val == '':
                                        unpopulated = True
                                    eval_List[0] = eval_List[0].replace('{}', str(val), 1)
                                if unpopulated:
                                    column_Vals_List[column] = ''
                                else:
                                    column_Vals_List[column] = str(eval(eval_List[0]))

                                derived = False


                            if ldd_Type_Conversion == "D.EXEC":
                                unpopulated = False
                                exec_List = ldd_Type_Format.split('|')
                                for ldd_Fields in exec_List[1:]:
                                    fieldNum = column_Header_List.index(ldd_Fields)
                                    val = (column_Vals_List[fieldNum])
                                    if val == '':
                                        unpopulated = True
                                    exec_List[0] = exec_List[0].replace('{}', str(val), 1)
                                if unpopulated:
                                    column_Vals_List[column] = ''
                                else:
                                    _ER_ = ''
                                    exec_List[0] = exec_List[0].split('#')
                                    exec_List[0] = '\n'.join(exec_List[0])
                                    exec_List[0] = exec_List[0].replace('\\t', '    ')
                                    exec(exec_List[0])
                                    column_Vals_List[column] = str(_ER_)

                                derived = False


#                                outline = ",".join(column_Vals_List)                                            # debug
#                                print outline                                                                   # debug

                            if (ldd_Type_Conversion == "D.DELTA_MONTHS") or (ldd_Type_Conversion == "D.DELTA_DAYS"):
                                timeDeltaFlag = False
                                unpopulated = False
                                date_List = []
                                eval_List = ldd_Type_Format.split('|')
                                for ldd_Fields in eval_List:
                                    if ldd_Fields == '_NOW_':
                                        date_List.append(date.today())
                                    elif ldd_Fields == '_UPDATE_DATE_':
                                        if updateStr == None:
                                            print "No --update in command line or g_Update global in keyfile."
                                            sys.exit(1)
                                        else:
                                            date_List.append(updateDateTime)

                                    elif ldd_Fields.endswith(':d', -2) == True:
                                        timeDeltaDays = timedelta(days=int(ldd_Fields[0:-2]))
                                        date_List.append(timeDeltaDays)
                                        timeDeltaFlag = True

                                    elif ldd_Fields.endswith(':m', -2) == True:
                                        timeDeltaDays = timedelta(days=int(int(ldd_Fields[0:-2]) * k_Number_Days_Monthly))
                                        date_List.append(timeDeltaDays)
                                        timeDeltaFlag = True

                                    else:
                                        fieldNum = column_Header_List.index(ldd_Fields)
                                        val = column_Vals_List[fieldNum]
                                        if val == '':
                                            unpopulated = True
                                        else:
                                            date_List.append(datetime.strptime(val, '%Y-%m-%d').date())

                                if unpopulated:
                                    column_Vals_List[column] = ''
                                elif timeDeltaFlag == False:
                                    deltaDays = (date_List[0] - date_List[1]).days
                                    if ldd_Type_Conversion == "D.DELTA_MONTHS":
                                        deltaDays /= k_Number_Days_Monthly                    # 365.242 / 12
                                    column_Vals_List[column] = str(int(deltaDays))
                                else:
                                    deltaDate = (date_List[0] - date_List[1])
                                    column_Vals_List[column] = deltaDate.strftime('%Y-%m-%d')

                                derived = False


                            if ldd_Type_Conversion == "D.DATE_OUT":
                                unpopulated = False
                                eval_List = ldd_Type_Format.split('|')
                                for ldd_Fields in eval_List[1:]:
                                    if ldd_Fields == '_NOW_':
                                        date_Out = date.today()
                                    elif ldd_Fields == '_UPDATE_DATE_':
                                        if updateStr == None:
                                            print "No --update in command line or g_Update global in keyfile."
                                            sys.exit(1)
                                        else:
                                            date_Out = updateDateTime
                                    else:
                                        fieldNum = column_Header_List.index(ldd_Fields)
                                        val = (column_Vals_List[fieldNum])
                                        if val == '':
                                            unpopulated = True
                                        else:
                                            date_Out = datetime.strptime(val, '%Y-%m-%d').date()
                                if unpopulated:
                                    column_Vals_List[column] = ''
                                else:
                                    column_Vals_List[column] = date_Out.strftime(eval_List[0])    # todo:  undefined

                                derived = False


#                                outline = ",".join(column_Vals_List)                                            # debug
#                                print outline                                                                   # debug

            # Filters
            filtered = False
            for filterRow in filterList:
                filterField = column_Header_List.index(filterRow[0])

                if filterRow[1] == 'EQUAL':
                    filterValue = str(filterRow[2])
                    if filterValue != str(column_Vals_List[filterField]):
                        filtered = True
                        break
#                   else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED EQUAL:  " + outline                                               # debug

                if filterRow[1] == 'NUMERIC.LT':
                    filterValue = float(filterRow[2])
                    if filterValue < float(column_Vals_List[filterField]) + 1e-9:
                        filtered = True
                        break
#                   else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED EQUAL:  " + outline                                               # debug

                if filterRow[1] == 'NUMERIC.GT':
                    filterValue = float(filterRow[2])
                    if filterValue + 1e-9 > float(column_Vals_List[filterField]) :
                        filtered = True
                        break
#                   else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED EQUAL:  " + outline                                               # debug

                if filterRow[1] == 'AFTER_DATE':
                    filterDate = datetime.strptime(filterRow[2], "%Y-%m-%d").date()
                    valDate = datetime.strptime(column_Vals_List[filterField], "%Y-%m-%d").date()
                    if filterDate > valDate:
                        filtered = True
                        break
#                    else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED AFTER_DATE:  " + outline                                          # debug

                elif filterRow[1] == 'BEFORE_DATE':
                    filterDate = datetime.strptime(filterRow[2], "%Y-%m-%d").date()
                    valDate = datetime.strptime(column_Vals_List[filterField], "%Y-%m-%d").date()
                    if filterDate < valDate:
                        filtered = True
                        break
#                    else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED BEFORE_DATE:  " + outline                                            #debug

                elif filterRow[1] == 'INCLUDE_CATEGORIES':
                    categories = filterRow[2].split('|')

                    matched = False
                    for category in categories:
                        if category == column_Vals_List[filterField]:
                            matched = True
                            break

                    if not matched:
                        filtered = True
#                    else:
#                        outline = ",".join(column_Vals_List)                                            # debug
#                        print "PASSED INCLUDE_CATEGORIES  " + outline                                    #debug
                        break

                elif filterRow[1] == 'SPECIFY':
                    if not specifyFilter == filterRow[2]:
                        specify_List = specify(filterRow[2])
                        specifyFilter = filterRow[2]
                    if not column_Vals_List[filterField] in specify_List:                       # todo:  undefined
                        filtered = True
                    break

        if not filtered:

            for ml_Include in ml_Include_List:
                column_Val = column_Vals_List[column_Header_List.index(ml_Include)]
                if (g_ML_Format == 'arff') and (column_Val == ''):
                    column_Val = '?'
                elif (g_ML_Format == 'arff') and (' ' in column_Val):
                    column_Val = '"' + column_Val + '"'
                ml_Vals_List.append(column_Val)


#                ldd_Visible_Field_List.append(ldd_Field)
#                header = ",".join(ldd_Visible_Field_List)


            for column_Val in column_Vals_List:                 # todo:  ml, too?
                endQuote_Flag = False
                column_Val_Index = column_Vals_List.index(column_Val)
                if ',' in column_Val:
                    endQuote_Flag = True
                if '"' in column_Val:
                    new_Column_Val = column_Vals_List[column_Val_Index].replace('"', '""')
                    column_Vals_List[column_Val_Index] = new_Column_Val
                    endQuote_Flag = True
                if endQuote_Flag:
                    column_Vals_List[column_Val_Index] = '"' + column_Vals_List[column_Val_Index] + '"'

            for ldd_NonVisible_Index in ldd_NonVisible_Index_List:
                del column_Vals_List[ldd_NonVisible_Index]


            if maxSamples == crit_ctr:
                break

            outline = ",".join(column_Vals_List)
            ml_Outline = ",".join(ml_Vals_List)

            incrementingFraction += trainingFraction
            if round(incrementingFraction) == intFractionPart:
                intFractionPart += 1

                fOutTrain.write("{0}\n".format(outline))
                fOutTrain_ML.write("{0}\n".format(ml_Outline))
            else:
                fOutTest.write("{0}\n".format(outline))
                fOutTest_ML.write("{0}\n".format(ml_Outline))

            crit_ctr += 1

        continue

    fOutTrain.close()
    fOutTest.close()
    fOutTrain_ML.close()
    fOutTest_ML.close()

    print
    print"...Done with {0} samples.".format(crit_ctr)


    #
    # Summary Stats?
    #

    if args.summary:
        print
        print 'Computing Summary Statistics...'
    else:
        sys.exit(0)


    if globalDict.get('g_DataFrame_Index') != '':
        indexColumn = globalDict.get('g_DataFrame_Index')
    else:
        indexColumn = None
        print
        print "No index specified in the key file."

    fInTrain = open(outFileRoot + "Train_.csv", "r")
    df = pd.read_csv(fInTrain, index_col=indexColumn)   # Todo:  master file for analytics

    print 'Reading from CSV...'


    for col in ldd_Visible_Field_List:
        if col != 'id_Loan_ID':
            print
            print col
            print df[col].describe()






