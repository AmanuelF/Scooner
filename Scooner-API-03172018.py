import json
from bottle import run, get, template, route, post, request, put
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


#####################################################################
@route('/scooner-pubmed/_doc/_key', method='POST')  # decorator for POST method(keyword searching)
def search_by_keywords():
    """
        Use POST instead of GET as that is 
        what the Tomcat server is looking for
    """    
    keywords = request.forms.get('keywords')     #to accept parameter value from a user
    page = request.forms.get('page')    
    skip = ((int(page)-1)*10)
    client = Elasticsearch()

    response = client.search(
    index="scooner-pubmed",
    body=template('abstract_template', keywords=keywords, skip=skip)      #bottle template following elasticsearch_dsl format----look at 'abstract_template.tpl' file

    )
    
    
    results = []
    
    for hit in response['hits']['hits']:
        r = hit['_source']
        r['id'] = hit['_id']      
	results.append(r)
         
    return {'results' : results}

#########################################################################
@route('/scooner-pubmed/_doc/_ids', method='POST')
def search_by_id():

    pmids = request.forms.get('pmids')  # comma seperated list of _id terms
    
    client = Elasticsearch()

    response = client.search(
    index="scooner-pubmed",
    body=template('ID_query', pmids=pmids)          #bottle template following elasticsearch_dsl format----look at 'ID_query.tpl' file

    )

    results = []
    for hit in response['hits']['hits']:
        r = hit['_source']
        r['id'] = hit['_id']
        results.append(r)

    return {'results' : results}


#########################################################################
@route('/scooner-pubmed/_doc/_id')
def search_by_id():
    
    id = request.query.id     
     
    client = Elasticsearch()

    response = client.search(
    index="scooner-pubmed",
    body=template('ID_query', id = id)          #bottle template following elasticsearch_dsl format----look at 'ID_query.tpl' file

    )
    
    results = [] 
    
    for hit in response['hits']['hits']:
            
        results.append(hit['_source'])   
    
    return {'results' : results} 
########################################################################

@route('/scooner-pubmed/_doc/_minor')
def search_minor():
    
    minor = request.query.minor
    
    client = Elasticsearch()

    response = client.search(
    index="scooner-pubmed",
    body=template('MeSH_Minor', minor = minor)                         #bottle template following elasticsearch_dsl format----look at 'MeSH_Minor.tpl' file

    )
    
    results = [] 
    
    for hit in response['hits']['hits']:
             
        results.append(hit['_source'])   
            
    return {'results' : results}


#########################################################################
@route('/scooner-pubmed/_doc/_major')
def search_major():
    
    major = request.query.major
    
    client = Elasticsearch()

    response = client.search(
    index="scooner-pubmed",
    body=template('MeSH_Major', major = major)                        #bottle template following elasticsearch_dsl format----look at 'MeSH_Major.tpl' file

    )
    
    results = [] 
    
    for hit in response['hits']['hits']:
             
   	results.append(hit['_source'])         
    
    return {'results' : results}

#########################################################################
@route('/scooner-pubmed/_doc/search', method='POST')                        #decorator for POST method
def search():
        
        query = template('abstract_template', keyword=request.json['keyword'])
        search = Search.from_dict(json.loads(query))
        
        print(search.to_dict())
        
        
        return search.to_dict()
#########################################################################

    
run(host='localhost', port=8088, debug=True)       #socket to listen on

