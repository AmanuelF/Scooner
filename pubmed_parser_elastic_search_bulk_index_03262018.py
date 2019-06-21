import json
import gzip
from Bio import Entrez
from pprint import pprint
from Bio import SeqIO
from datetime import datetime
import calendar
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import glob              #for iterating through files in a directory


def get_actions():

    for filename in glob.glob('pubmed18n06**.xml.gz'):     #Eg. for iterating through dumps with the name format specified with wildcards
        try:
            handle = gzip.open(filename)
            record = Entrez.read(handle)
        except EOFError:                                    #skip End of File Errors while reading from a file
            pass   

####################################################################

"""
    #optional month name to month number conversion function
    
    #dictionary of month name-to-month number pairs
    #month_dict = {"Jan":1,"Feb":2,"Mar":3,"Apr":4, "May":5, "Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,
              "Nov":11,"Dec":12}
 
    #def to_dict(name):                              #Function to accept name of a month and return a month number
    #    return month_dict[name]

"""
 
        def to_if(name):                             #Function to accept name of a month and return corresponding integer representation
            if name == "Jan": return '01'
            elif name == "Feb": return '02'
            elif name == "Mar": return '03'
            elif name == "Apr": return '04'
            elif name == "May": return '05'
            elif name == "Jun": return '06'
            elif name == "Jul": return '07'
            elif name == "Aug": return '08'
            elif name == "Sep": return '09'
            elif name == "Oct": return '10'
            elif name == "Nov": return '11'
            elif name == "Dec": return '12'
 


        docID = 0                                  #variable to store ID of an article which equals PMID  


        for article in record['PubmedArticle']:
            abstractString = ''
            abstracttext = ''
            try:
                for abstract1 in article['MedlineCitation']['Article']['Abstract']['AbstractText']:
                
                    try:
                        abstracttext1 = abstract1.attributes['Label'] + ': ' + abstract1 + ' '
                    except:
                        abstracttext1 = abstract1
            
                    abstracttext = abstracttext + abstracttext1
                abstractString = abstracttext

            except:
                continue
 
            globalAbstract = abstractString

    
    ###########################################################################################
            keyString = ''
            keyList = []
        
            try:
                for keylist in article['MedlineCitation']['KeywordList']:
                    for key in keylist:
                        keyList.append(str(key))
                    
            except:
                keyList = []
        
            keyString = str(keyList)
        
    ##########################################################################################
    
    #these lines deal with the extraction of mesh_minor and mesh_major fields

            Major_List = []
            Minor_List = []
        
            try:
                for MeSH in article['MedlineCitation']['MeshHeadingList']:
                    if MeSH['DescriptorName'].attributes['MajorTopicYN'] == 'N':
                        Minor_List.append(MeSH['DescriptorName'])
                    elif MeSH['DescriptorName'].attributes['MajorTopicYN'] == 'Y':
                        Major_List.append(MeSH['DescriptorName'])
            except:
                Major_List = []
                Minor_List = []
    
    
    ##########################################################################################
    
    
        #extraction of article title from the dump
            articleList = ('title:'  + article['MedlineCitation']['Article']['ArticleTitle']).split(':')    
    
    
            try:
            
                #lines to deal with part of 'PubDate' tag where 'MedlineDate' appears
                dateString = article['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['MedlineDate'].split('-')[0].split(' ')
                year = dateString[0]
                month = dateString[1]
                monthNumber = str(to_if(month))
        
                if(monthNumber == 'None'):
                    monthNumber = '01'        
        
                day = '01'
                dateString = dateString = year + '' + monthNumber + '' + day
                dateList = datetime.strptime(dateString, '%Y%m%d')                #conversion into a datetime object   
                date = str(dateList.date())                                  #conversion into a string representation of the date
    
            except:
        
                try:
                    date = article['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['Day']    #extract day 
                except:
                    date = '01'
    
                try:
                    month = article['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['Month']   #extract month 
                    monthNumber = str(to_if(month))
        
                    if(monthNumber == 'None'):    #if monthNumber is not specified in the 'Month' field, a monthNumber of '01' is assumed
                        monthNumber = '01'                
                except:
                    month = "Dec"
                    monthNumber = str(to_if(month))      #conversion into a number representation of a month
    
    
    
                try:
                    year = article['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['Year']  #to extract the year
        
                    dateString = year + '' + monthNumber + '' + date
        
                    dateList = datetime.strptime(dateString, '%Y%m%d')    #conversion into a datetime object    
                    date = str(dateList.date())    
        
                    
                except KeyError:        #continue if key is not available
                    continue
    

            docID = article['MedlineCitation']['PMID']
            titleString = article['MedlineCitation']['Article']['ArticleTitle']
            abstractString = str(globalAbstract)
    
    ################################################################
    
    #code to generate document for each corresponding pubmed article

            action = {
            '_op_type': 'index',
            '_index': 'scooner-pubmed',
            '_type': '_doc',
            '_id': docID,
    
            '_source': {
                "title": titleString,
                "abstract": abstractString,
                "date": date,
                "datasource": "pubmed",
                "keywords": keyList,
                "mesh_major": Major_List,
                "mesh_minor": Minor_List
                }
            }  


        yield action      #yield an iterable    


    
#actions = []             #iterable object to be used for Elasticsearch bulk indexing
  
    
        #actions.append(action)      #append each document onto iterable 'actions'

es = Elasticsearch()                #define an Elasticsearch object

helpers.bulk(es, get_actions())           #bulk index into Elasticsearch


