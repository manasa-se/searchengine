
# Copyright (C) 2011 by Peter Goodman
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

#
# Last Updated: Oct 13, 2013
#


import urllib2 , urllib, httplib
import urlparse
from BeautifulSoup import *
from collections import *
import sqlite3
import re
from sets import Set
import numpy as np
from threading import Thread
import cProfile


def attr(elem, attr):
    """An html attribute from an html element. E.g. <a href="">, then
    attr(elem, "href") will get the href or an empty string."""
    try:
        return elem[attr]
    except:
        return ""

WORD_SEPARATORS = re.compile(r'\s|\n|\r|\t|[^a-zA-Z0-9\-_]')

def page_rank(links, num_iterations=20, initial_pr=1.0):

    page_rank = defaultdict(lambda: float(initial_pr))
    num_outgoing_links = defaultdict(float)
    incoming_link_sets = defaultdict(set)
    incoming_links = defaultdict(lambda: np.array([]))
    damping_factor = 0.85

    # collect the number of outbound links and the set of all incoming documents
    # for every document
    for (from_id,to_id) in links:
        num_outgoing_links[int(from_id)] += 1.0
        incoming_link_sets[to_id].add(int(from_id))
    
    # convert each set of incoming links into a numpy array
    for doc_id in incoming_link_sets:
        incoming_links[doc_id] = np.array([from_doc_id for from_doc_id in incoming_link_sets[doc_id]])

    num_documents = float(len(num_outgoing_links))
    if(num_documents):
        lead = (1.0 - damping_factor) / num_documents
    partial_PR = np.vectorize(lambda doc_id: page_rank[doc_id] / num_outgoing_links[doc_id])

    for _ in xrange(num_iterations):
        for doc_id in num_outgoing_links:
            tail = 0.0
            if len(incoming_links[doc_id]):
                tail = damping_factor * partial_PR(incoming_links[doc_id]).sum()
            page_rank[doc_id] = lead + tail
    
    return page_rank

class crawler(object):
    """Represents 'Googlebot'. Populates a database by crawling and indexing
    a subset of the Internet.

    This crawler keeps track of font sizes and makes it simpler to manage word
    ids and document ids."""
    c=0
    def __init__(self, db_conn, url_file):
        """Initialize the crawler with a connection to the database to populate
        and with the file containing the list of seed URLs to begin indexing."""
        self.c = db_conn.cursor()
        self._url_queue = [ ]
        self._doc_id_cache = { }
        self._word_id_cache = { }
        self._inverted_index_cache = { }
        self._resolved_inverted_index_cache = { }
        self._curr_title = "No title for this page"
        self._curr_description = "No description available"


        # functions to call when entering and exiting specific tags
        self._enter = defaultdict(lambda *a, **ka: self._visit_ignore)
        self._exit = defaultdict(lambda *a, **ka: self._visit_ignore)

        # add a link to our graph, and indexing info to the related page
        self._enter['a'] = self._visit_a

        # record the currently indexed document's title and increase
        # the font size
        def visit_title(*args, **kargs):
            self._visit_title(*args, **kargs)
            self._increase_font_factor(7)(*args, **kargs)

        # increase the font size when we enter these tags
        self._enter['b'] = self._increase_font_factor(2)
        self._enter['strong'] = self._increase_font_factor(2)
        self._enter['i'] = self._increase_font_factor(1)
        self._enter['em'] = self._increase_font_factor(1)
        self._enter['h1'] = self._increase_font_factor(7)
        self._enter['h2'] = self._increase_font_factor(6)
        self._enter['h3'] = self._increase_font_factor(5)
        self._enter['h4'] = self._increase_font_factor(4)
        self._enter['h5'] = self._increase_font_factor(3)
        self._enter['title'] = visit_title

        # decrease the font size when we exit these tags
        self._exit['b'] = self._increase_font_factor(-2)
        self._exit['strong'] = self._increase_font_factor(-2)
        self._exit['i'] = self._increase_font_factor(-1)
        self._exit['em'] = self._increase_font_factor(-1)
        self._exit['h1'] = self._increase_font_factor(-7)
        self._exit['h2'] = self._increase_font_factor(-6)
        self._exit['h3'] = self._increase_font_factor(-5)
        self._exit['h4'] = self._increase_font_factor(-4)
        self._exit['h5'] = self._increase_font_factor(-3)
        self._exit['title'] = self._increase_font_factor(-7)

        # never go in and parse these tags
        self._ignored_tags = set([
            'meta', 'script', 'link', 'meta', 'embed', 'iframe', 'frame', 
            'noscript', 'object', 'svg', 'canvas', 'applet', 'frameset', 
            'textarea', 'style', 'area', 'map', 'base', 'basefont', 'param',
        ])

        # set of words to ignore
        self._ignored_words = set([
            '', 'the', 'of', 'at', 'on', 'in', 'is', 'it',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
            'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z', 'and', 'or',
        ])


        # keep track of some info about the page we are currently parsing
        self._curr_depth = 0
        self._curr_url = ""
        self._curr_doc_id = 0
        self._font_size = 0
        self._curr_words = None
        self._curr_page_rank_list = list()
        

        # get all urls into the queue
        try:
            with open(url_file, 'r') as f:
                for line in f:
                    self._url_queue.append((self._fix_url(line.strip(), ""), 0))
        except IOError:
            pass
    

    def _mock_insert_document(self, url):
        """A function that inserts a url into a document db table
        and then returns that newly inserted document's id."""
        if (not url):
            return 0
        
        self.c.execute("SELECT * FROM DOCUMENT_INDEX WHERE url LIKE (?)",(url,))
        result = self.c.fetchall()
        if (not(result and len(result))):
            self.c.execute('INSERT INTO DOCUMENT_INDEX ("url") VALUES (?)', (url,))
        self.c.execute("SELECT id FROM DOCUMENT_INDEX WHERE url LIKE (?)",(url,))
        result = self.c.fetchall()
        
        ret_id = int(result[0][0])
        
        return ret_id
    

    def _mock_insert_word(self, word):
        """A function that inserts a word into the lexicon db table
        and then returns that newly inserted word's id."""

        ret_id = -1
        self.c.execute("SELECT * FROM LEXICON WHERE word LIKE (?)",(word,))
        result = self.c.fetchall()
        if (not(result and len(result))):
            self.c.execute('INSERT INTO LEXICON ("word") VALUES (?)', (word,))
        self.c.execute("SELECT id FROM LEXICON WHERE word LIKE (?)",(word,))
        result = self.c.fetchall()

        ret_id = int(result[0][0])
        
        return ret_id

    def _mock_insert_inverted_index(self):
        """creates a an inverted index where the given word maps
        to a list of document ids"""

        #empty the contents of the inverted index, as new word ids might have been generated
        self.c.execute("DELETE FROM INVERTED_INDEX")
        self.c.execute('SELECT d.id as doc_id, l.id as word_id FROM document_index d join lexicon l on d.word_id_list like ("%(" || l.id || ",%") order by l.id')
        result = self.c.fetchall()
        #result now holds a table with each word and a document that it appears in

        #now we will combine all the documents for a word into a doc_id_list

        doc_id_string = ""
        inverted_index = { }
        
        temp_word = result[0][1]

        for each_row in result:
            if (each_row[1] != temp_word):
                if len(doc_id_string):
                    doc_id_string = doc_id_string[0:len(doc_id_string)-1]
                    inverted_index[temp_word] = doc_id_string
                temp_word = each_row[1]
                doc_id_string = str(each_row[0]) + ","

            else:
                doc_id_string = doc_id_string + str(each_row[0])+','
        inverted_index[temp_word] = doc_id_string[0:len(doc_id_string)-1]
        
        #here we populate the inverted index
        for key in inverted_index:
            self.c.execute("INSERT INTO INVERTED_INDEX ('word_id','doc_id_list') VALUES (?,?)",((str(key)),(inverted_index[key])))

        if (inverted_index):
            return inverted_index
       
    
    def word_id(self, word):
        """Get the word id of some specific word."""
        if word in self._word_id_cache:
            return self._word_id_cache[word]
        

        # TODO: 1) add the word to the lexicon, if that fails, then the
        #          word is in the lexicon
        #       2) query the lexicon for the id assigned to this word, 
        #          store it in the word id cache, and return the id.

        word_id = self._mock_insert_word(word)
        return word_id
    
    def document_id(self, url):
        """Get the document id for some url."""
        if url in self._doc_id_cache:
            return self._doc_id_cache[url]
        
        # TODO: just like word id cache, but for documents. if the document
        #       doesn't exist in the db then only insert the url and leave
        #       the rest to their defaults.
        
        doc_id = self._mock_insert_document(url)
        self._doc_id_cache[url] = doc_id
        return doc_id

    


    
    def _fix_url(self, curr_url, rel):
        """Given a url and either something relative to that url or another url,
        get a properly parsed url."""

        rel_l = rel.lower()
        if rel_l.startswith("http://") or rel_l.startswith("https://"):
            curr_url, rel = rel, ""
            
        # compute the new url based on import 
        curr_url = urlparse.urldefrag(curr_url)[0]
        parsed_url = urlparse.urlparse(curr_url)
        return urlparse.urljoin(parsed_url.geturl(), rel)

    
    def get_inverted_index(self,word_id_optional = -1):

        #here we create the inverted_index_cache as a set wher key is word_id and value is comma separated doc_id_list 
        temp_cache = self._mock_insert_inverted_index()
        for key in temp_cache:
            doc_id_set = Set()
            value = temp_cache[key].split(',')
            for doc_id in value:
                doc_id_set.add(doc_id)
            self._inverted_index_cache[key] = doc_id_set
            
        #if the function is called without parameter, print the entire cache else print the doc_id_list for the specified word_id
        if(word_id_optional == -1):
            print self._inverted_index_cache
        else:
            print str(word_id_optional) + "->" 

            print self._inverted_index_cache[word_id_optional]


    def get_resolved_inverted_index(self,word_id_optional =""):
        #does the same thing as get_inverted_cache, but replaces word_id with word and doc_id_list with a list of urls
        temp_cache = self._mock_insert_inverted_index()
        for key in temp_cache:
            doc_val_set = Set()
            value = temp_cache[key].split(',')
            for doc_id in value:
                self.c.execute("SELECT URL FROM DOCUMENT_INDEX WHERE ID =" +doc_id)
                doc_val = self.c.fetchall()
                doc_val_set.add(str(doc_val[0][0]))
            self.c.execute("SELECT word FROM lexicon WHERE ID = " + str(key))
            word_val = str(self.c.fetchall()[0][0])
            self._resolved_inverted_index_cache[word_val] = doc_val_set
        #if function called without parameter, it prints the enitre cache, else print only the list of urls for the word provided
        if(not(word_id_optional)):
            print self._resolved_inverted_index_cache
        else:
            print word_id_optional + "->"
            print self._resolved_inverted_index_cache[word_id_optional]


    def print_bonus(self,url=""):

        #here we print the title and a description of a page 

        #if no url is provided int he function call, then print title and description for each url in document_index and return the result
        if(not(url)):
            self.c.execute("SELECT url,title,description FROM DOCUMENT_INDEX")
            result = self.c.fetchall()
            for row in result:
                print row
                print "\n"
            return result

        #if url provided print the title and description for the specified url
        self.c.execute("SELECT title,description FROM DOCUMENT_INDEX WHERE url ='" + url + "'")
        result = self.c.fetchall()

        bonus = ""
        if (not(result)):
            bonus = str(url) + " => " "title = " + str(result[0][0]) + ", description = " + str(result[0][1])
            print bonus
        return bonus


    def add_link(self, from_doc_id, to_doc_id):
        """Add a link into the database, or increase the number of links between
        two pages in the database."""
        # TODO
        

    def _visit_title(self, elem):
        """Called when visiting the <title> tag."""
        #store title for a webpage in _curr_title
        title_text = self._text_of(elem).strip()
        if len(title_text) > 0 :
            self._curr_title = title_text
        else: 
            self._curr_title = "No title Available"
        
        #print "document title="+ repr(title_text)

        # TODO update document title for document id self._curr_doc_id
    
    def _visit_a(self, elem):
        """Called when visiting <a> tags."""

        dest_url = self._fix_url(self._curr_url, attr(elem,"href"))

        #print "href="+repr(dest_url), \
        #      "title="+repr(attr(elem,"title")), \
        #      "alt="+repr(attr(elem,"alt")), \
        #      "text="+repr(self._text_of(elem))
        
        substr = "javascript"
        if ( not (substr in dest_url)):
        # add the just found URL to the url queue
            self._url_queue.append((dest_url, self._curr_depth))
            self.add_link(self._curr_doc_id, self.document_id(dest_url))
            link = (self._curr_doc_id,self.document_id(dest_url))
            if link not in self._curr_page_rank_list:
                self._curr_page_rank_list.append(link)


        # add a link entry into the database from the current document to the
        # other document
            

        # TODO add title/alt/text to index for destination url
    
    def _add_words_to_document(self):
        """TODO: knowing self._curr_doc_id and the list of all words and their
               font sizes (in self._curr_words), add all the words into the
               database for this document

        print "    num words="+ str(len(self._curr_words)"""
        #Here we update document index by adding the word_id_list, title and description for each url
        self.c.execute('UPDATE DOCUMENT_INDEX SET word_id_list = (?),title= (?),description= (?) WHERE id ='+str(self._curr_doc_id ),
        (str(self._curr_words).strip('[]'),self._curr_title,self._curr_description))

        

        
        


    def _increase_font_factor(self, factor):
        """Increase/decrease the current font size."""
        def increase_it(elem):
            self._font_size += factor
        return increase_it
    
    def _visit_ignore(self, elem):
        """Ignore visiting this type of tag"""
        pass

    def _add_text(self, elem):
        """Add some text to the document. This records word ids and word font sizes
        into the self._curr_words list for later processing."""
        words = WORD_SEPARATORS.split(elem.string.lower())
        for word in words:
            word = word.strip()
            if word in self._ignored_words:
                continue
            self._curr_words.append((self.word_id(word), self._font_size))
        
    def _text_of(self, elem):
        """Get the text inside some element without any tags."""
        if isinstance(elem, Tag):
            text = [ ]
            for sub_elem in elem:
                text.append(self._text_of(sub_elem))
            
            return " ".join(text)
        else:
            return elem.string

    def _index_document(self, soup):
        """Traverse the document in depth-first order and call functions when entering
        and leaving tags. When we come accross some text, add it into the index. This
        handles ignoring tags that we have no business looking at."""
        class DummyTag(object):
            next = False
            name = ''
        
        class NextTag(object):
            def __init__(self, obj):
                self.next = obj
        
        tag = soup.html
        stack = [DummyTag(), soup.html]

        while tag and tag.next:
            tag = tag.next

            # html tag
            if isinstance(tag, Tag):

                if tag.parent != stack[-1]:
                    self._exit[stack[-1].name.lower()](stack[-1])
                    stack.pop()

                tag_name = tag.name.lower()

                # ignore this tag and everything in it
                if tag_name in self._ignored_tags:
                    if tag.nextSibling:
                        tag = NextTag(tag.nextSibling)
                    else:
                        self._exit[stack[-1].name.lower()](stack[-1])
                        stack.pop()
                        tag = NextTag(tag.parent.nextSibling)
                    
                    continue
                
                # enter the tag
                self._enter[tag_name](tag)
                stack.append(tag)

            # text (text, cdata, comments, etc.)
            else:
                self._add_text(tag)

    def crawl(self, depth=1, timeout=3):
        """Crawl the web!"""
        seen = set()

        while len(self._url_queue):

            url, depth_ = self._url_queue.pop()

            # skip this url; it's too deep
            if depth_ > depth:
                continue

            doc_id = self.document_id(url)

            # we've already seen this document
            if doc_id in seen:
                continue

            seen.add(doc_id) # mark this document as haven't been visited
            
            socket = None
            try:
                socket = urllib2.urlopen(url, timeout=timeout)
                
                soup = BeautifulSoup(socket.read())

                self._curr_depth = depth_ + 1
                self._curr_url = url
                self._curr_doc_id = doc_id
                self._font_size = 0
                self._curr_words = [ ]


                

                

                

                #get description of the current url in _curr_description
                tempDescription = soup.findAll(attrs={"name":"description"})
                if len(tempDescription) > 0:
                    self._curr_description = tempDescription[0]['content']
                else:
                    self._curr_description = "No Description Available"

                self._index_document(soup)
                self._add_words_to_document()
                print "    url="+repr(self._curr_url)

            except Exception as e:
                print e
                pass
            finally:
                if socket:
                    socket.close()

    def create_page_rank(self):
        default_pr = 0.0
        #print self._curr_page_rank_list
        temp = page_rank(self._curr_page_rank_list)
        for (key,value) in temp.items():  
            self.c.execute('UPDATE DOCUMENT_INDEX SET page_rank = ' + str(value)+ ' WHERE id =' + str(key)) 
            

        


if __name__ == "__main__":
    con = sqlite3.connect('quaero.db') # Warning: This file is created in the current directory
    
    #now we will drop the tables before creating them
    
    con.execute("DROP TABLE IF EXISTS DOCUMENT_INDEX")
    con.execute("DROP TABLE IF EXISTS LEXICON")
    con.execute("DROP TABLE IF EXISTS INVERTED_INDEX")

    #Now we create new empty tables hat will be populated by the crawler
    con.execute("CREATE TABLE IF NOT EXISTS LEXICON (id INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL , word VARCHAR NOT NULL  UNIQUE )")
    con.execute("CREATE  TABLE IF NOT EXISTS DOCUMENT_INDEX (id INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL , url VARCHAR NOT NULL  UNIQUE , word_id_list VARCHAR, title VARCHAR , description VARCHAR, page_Rank REAL DEFAULT 0.0)")
    con.execute("CREATE TABLE IF NOT EXISTS INVERTED_INDEX (word_id INTEGER PRIMARY KEY UNIQUE NOT NULL , doc_id_list VARCHAR )")
    
    con.commit()
    con.isolation_level = None


    
    
    bot = crawler(con, "urls.txt")
    bot.crawl(depth=1)

    bot.get_inverted_index()
    #bot.get_resolved_inverted_index()
    #bot.print_bonus()
    bot.create_page_rank()

    con.close()

