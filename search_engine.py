from bottle import route, run, debug, template,request, static_file
from collections import *
from bottle import error
import sqlite3
from timer import Timer

keywordSearchCount = Counter()

@route('/')
def homepage():
    return template('search_page',phrase = "",topKeywords = keywordSearchCount.most_common(20))

@route('/ajaxUpdateTopKeywords')
def ajaxUpdateTopKeywords():
    phrase = request.GET.get('searchInput')
    wordsArray = phrase.split()
    for word in wordsArray:
        keywordSearchCount[word.lower()] += 1   
    return template('top_keywords',topKeywords = keywordSearchCount.most_common(20))

@route('/ajaxSearchResults')
def ajaxSearchResults():
    #reset static variables if search button is pressed
    if request.GET.get('from') != None and request.GET.get('from') == "form_submit":
        ajaxSearchResults.prevSearchedWord = request.GET.get('searchInput').split('=')[1]
        with Timer() as t:
            ajaxSearchResults.searchResults = retrieveResults(ajaxSearchResults.prevSearchedWord.split("+")[0])
        print "=> elasped searchResults: %s s" % t.secs
        ajaxSearchResults.counter = 0
        
    #retrieve resutls in groups of 10
    if len(ajaxSearchResults.searchResults) >= ajaxSearchResults.counter:
        results = ajaxSearchResults.searchResults[ajaxSearchResults.counter:ajaxSearchResults.counter+10]
        ajaxSearchResults.counter += 10
        return template('search_results',searchResults = results)
ajaxSearchResults.searchResults = []
ajaxSearchResults.prevSearchedWord = ""
ajaxSearchResults.counter = 0


@error(404)
def error404(error):
    return template('error_page')

@route('/static/<filename>')
def server_static(filename):
  return static_file(filename, root='./static/')

def retrieveResults(word):
    if word not in retrieveResults.cacheResults:
        con = sqlite3.connect('quaero.db')
        con.text_factory = str
        cur = con.cursor()
        cur.execute("select doc_id_list from inverted_index where word_id = (select id from lexicon where lower(word) like '%"+ word.lower()+"%')")
        doc_id_list = cur.fetchone()
        searchResults = []
        if doc_id_list != None:
            cur.execute("select url,title,description from document_index where id in ("+ doc_id_list[0] +") order by page_rank desc")
            searchResults = cur.fetchall()
        con.close()
        if len(retrieveResults.cacheResults) > 100:
            retrieveResults.cacheResults.clear()
        retrieveResults.cacheResults[word] = searchResults
    return retrieveResults.cacheResults[word]
retrieveResults.cacheResults = {}

run(reloader=True, host='localhost', port=8080)