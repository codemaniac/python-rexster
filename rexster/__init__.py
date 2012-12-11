#!/usr/bin/env python
#-*- coding:utf-8 -*-

import requests
import simplejson

# Rexster v2.2.0-SNAPSHOT tested !
class RexsterException(BaseException):
    pass

# Rexster v2.2.0-SNAPSHOT tested !
class RexsterServer(object):
    """An class that implements a way to connect to
    a Rexster Instance from Python"""
    def __init__(self, host):
        self.host = host
        r = requests.get(host)
        if r.error:
            raise RexsterException("Could not connect to a Rexster server")
        else:
            self.data = simplejson.loads(r.content)

    def name(self):
        """Return server name"""
        return self.data.get('name')

    def version(self):
        """Return server version"""
        return self.data.get('version')

    def uptime(self):
        """Return server uptime"""
        return self.data.get('upTime')

    def graphs(self):
        """Returns a list of available graphs"""
        return self.data.get('graphs')

# Rexster v2.2.0-SNAPSHOT tested !
class Element(object):
    """An class defining an Element object composed
    by a collection of key/value properties for the
    Rexster compatible database"""

    # Rexster v2.2.0-SNAPSHOT tested !
    def __init__(self, graph, url):
        """Creates a new element
        @params graph: The graph object the element belongs
        @params url: The element REST URL

        @returns The element"""
        self.url = url
        self.graph = graph
        r = requests.get(url)
        content = simplejson.loads(r.content)
        properties = content.get('results')
        if not properties:
            raise RexsterException(content['message'])
        self.properties = {}
        for key, value in properties.iteritems():
            self.properties[key] = value
        self._id = self.properties.get('_id')

    # Rexster v2.2.0-SNAPSHOT tested !
    def getId(self):
        """Returns the unique identifier of the element

        @returns The unique identifier of the element"""
        return self._id

    # Rexster v2.2.0-SNAPSHOT tested !
    def updateProperties(self, **params):
        """Updates the properties of the element to the given value
        @params params: The property key-value dictionary to set"""
        for key, value in params.iteritems():
          self.properties[key] = value

        url_frag = ''
        for key, value in self.properties.iteritems():          
          url_frag += '%s=%s&' % (key,value)
          
        r = requests.put(self.url + '?' + url_frag)
        if r.error:
            error_msg = simplejson.loads(r.content)['message']
            raise RexsterException(error_msg)

    # Rexster v2.2.0-SNAPSHOT tested !        
    def getProperty(self, key):
        """Gets the value of the property for the given key
        @params key: The key which value is being retrieved

        @returns The value of the property with the given key"""
        try:
          return self.properties[key]         
        except KeyError:
          raise RexsterException('No such property for element')

    # Rexster v2.2.0-SNAPSHOT tested !
    def getPropertyKeys(self):
        """Returns a set with the property keys of the element

        @returns Set of property keys"""
        return self.properties.keys()

    # Rexster v2.2.0-SNAPSHOT tested !
    def removeProperties(self, *keys):
        """Removes the value of the property for the given key
        @params key: The key which value is being removed"""        
        r = requests.delete(self.url + '?' + '&'.join(keys))
        if r.error:
            error_msg = simplejson.loads(r.content)['message']
            raise RexsterException(error_msg)
        else:
          for key in keys:
            del self.properties[key]

    # Rexster v2.2.0-SNAPSHOT tested !
    def __eq__(self, other):
      if type(self) == type(other):
        return self.getId() == other.getId()
      else:
        return False

# Rexster v2.2.0-SNAPSHOT tested !
class Vertex(Element):
    """An abstract class defining a Vertex object representing
    a node of the graph with a set of properties"""

    # Rexster v2.2.0-SNAPSHOT tested !
    def __init__(self, graph, _id):
        """Creates a new vertex
        @params graph: The graph object the vertex belongs
        @params _id: The vertex unique identifier

        @returns The vertex"""
        url = "%s/vertices/%s" % (graph.url, _id)
        super(Vertex, self).__init__(graph, url)

    # Rexster v2.2.0-SNAPSHOT tested !
    def _generator(self, generator):
        for item in generator:
            yield Edge(self.graph, item.get('_id'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def getOutEdges(self, label=None):
        """Gets all the outgoing edges of the node. If label
        parameter is provided, it only returns the edges of
        the given label
        @params label: Optional parameter to filter the edges

        @returns A generator function with the outgoing edges"""
        if label:
            url = "%s/outE?_label=%s" % (self.url, label)
        else:
            url = "%s/outE" % self.url
        r = requests.get(url)
        return self._generator(simplejson.loads(r.content)['results'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def getInEdges(self, label=None):
        """Gets all the incoming edges of the node. If label
        parameter is provided, it only returns the edges of
        the given label
        @params label: Optional parameter to filter the edges

        @returns A generator function with the incoming edges"""
        if label:
            url = "%s/inE?_label=%s" % (self.url, label)
        else:
            url = "%s/inE" % self.url
        r = requests.get(url)
        return self._generator(simplejson.loads(r.content)['results'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def getBothEdges(self, label=None):
        """Gets all the edges of the node. If label
        parameter is provided, it only returns the edges of
        the given label
        @params label: Optional parameter to filter the edges

        @returns A generator function with the incoming edges"""
        if label:
            url = "%s/bothE?_label=%s" % (self.url, label)
        else:
            url = "%s/bothE" % self.url
        r = requests.get(url)
        return self._generator(simplejson.loads(r.content)['results'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def __str__(self):
        return "Vertex %s: %s" % (self._id, self.properties)

# Rexster v2.2.0-SNAPSHOT tested !
class Edge(Element):
    """An abstract class defining a Edge object representing
    a relationship of the graph with a set of properties"""

    # Rexster v2.2.0-SNAPSHOT tested !
    def __init__(self, graph, _id):
        """Creates a new edge
        @params graph: The graph object the edge belongs
        @params _id: The edge unique identifier

        @returns The edge"""
        url = "%s/edges/%s" % (graph.url, _id)
        super(Edge, self).__init__(graph, url)

    # Rexster v2.2.0-SNAPSHOT tested !
    def getOutVertex(self):
        """Returns the origin Vertex of the relationship

        @returns The origin Vertex"""
        return Vertex(self.graph, self.properties.get('_outV'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def getInVertex(self):
        """Returns the target Vertex of the relationship

        @returns The target Vertex"""
        return Vertex(self.graph, self.properties.get('_inV'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def getLabel(self):
        """Returns the label of the relationship

        @returns The edge label"""
        return self.properties.get('_label')

    # Rexster v2.2.0-SNAPSHOT tested !
    def __str__(self):
        return "Edge %s: %s" % (self._id, self.properties)

# Rexster v2.2.0-SNAPSHOT tested !
class RexsterGraph(object):

    # Rexster v2.2.0-SNAPSHOT tested !
    def __init__(self, server, name):
        self.server = server
        self.name = name
        self.url = "%s/%s" % (server.host, name)

    # Rexster v2.2.0-SNAPSHOT tested !
    def getMetadata(self):
        r = requests.get(self.url)
        return simplejson.loads(r.content)

    # Rexster v2.2.0-SNAPSHOT tested !
    def addVertex(self, _id=None, properties=None):
        """Adds a new vertex
        @params _id: Node unique identifier

        @returns The created Vertex or None"""
        if _id:
            url = "%s/vertices/%s" % (self.url, _id)
        else:
            url = "%s/vertices" % (self.url)
        r = requests.post(url)
        if r.error:
            raise RexsterException("Could not create vertex")
        else:
            props = simplejson.loads(r.content)['results']
            v = Vertex(self, props['_id'])
            v.updateProperties(**properties)
            return v

    # Rexster v2.2.0-SNAPSHOT tested !
    def getVertex(self, _id):
        """Retrieves an existing vertex from the graph
        @params _id: Node unique identifier

        @returns The requested Vertex or None"""
        try:
            return Vertex(self, _id)
        except RexsterException:
            return None

    # Rexster v2.2.0-SNAPSHOT tested !
    def getKeyIndexedVertices(self, key, value):
        """get all vertices for a key index given the specified <key>/<value>
        @params key: Vertex key string
        @params value: Vertex value string

        @returns The requested Vertices or None"""
        try:
            requrl = "%s/vertices?key=%s&value=%s" % (self.url, key, value)
            r = requests.get(requrl)
            if r.error:
              raise RexsterException("Could retrieve indexed vertices")
            for vertex in simplejson.loads(r.content)['results']:
              yield Vertex(self, vertex.get('_id'))            
        except KeyError:
            raise RexsterException("Could retrieve indexed vertices")

    # Rexster v2.2.0-SNAPSHOT tested !
    def getVertices(self):
        """Returns an iterator with all the vertices"""
        url = "%s/vertices" % self.url
        r = requests.get(url)
        for vertex in simplejson.loads(r.content)['results']:
            yield Vertex(self, vertex.get('_id'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def removeVertex(self, vertex):
        """Removes the given vertex
        @params vertex: Node to be removed"""
        _id = vertex.getId()
        url = "%s/vertices/%s" % (self.url, _id)
        r = requests.delete(url)
        if r.error:
            raise RexsterException("Could not delete vertex")

    # Rexster v2.2.0-SNAPSHOT tested !
    def addEdge(self, outV, inV, label, properties=None):
        """Creates a new edge
        @params outVertex: Edge origin Vertex
        @params inVertex: Edge target vertex
        @params label: Edge label

        @returns The created Edge object"""
        url = "%s/edges?_outV=%s&_inV=%s&_label=%s" % (self.url,
                                                    outV.getId(),
                                                    inV.getId(),
                                                    label)
        data = dict(_outV=outV.getId(),
                    _inV=inV.getId(),
                    _label=label)
        r = requests.post(url, data=data)
        if r.error:
            raise RexsterException("Could not create the edge")
        props = simplejson.loads(r.content)['results']
        e = Edge(self, props['_id'])
        e.updateProperties(**properties)
        return e

    # Rexster v2.2.0-SNAPSHOT tested !
    def getEdges(self):
        """Returns an iterator with all the edges"""
        url = "%s/edges" % self.url
        r = requests.get(url)
        content = simplejson.loads(r.content)
        if r.error:
            raise RexsterException(content['message'])
        else:
            for edge in content['results']:
                yield Edge(self, edge.get('_id'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def getEdge(self, _id):
        """Retrieves an existing edge from the graph
        @params _id: Edge unique identifier

        @returns The requested Edge"""
        try:
            return Edge(self, _id)
        except RexsterException:
            return None

    # Rexster v2.2.0-SNAPSHOT tested !
    def removeEdge(self, edge):
        """Removes the given edge
        @params edge: The edge to be removed"""
        _id = edge.getId()
        url = "%s/edges/%s" % (self.url, _id)
        r = requests.delete(url)
        if r.error:
            raise RexsterException("Could not delete edge")

    # Rexster v2.2.0-SNAPSHOT tested !
    def gremlin_execute(self, gremlin_script):
        url = '%s/tp/gremlin?script=%s' % (self.url, gremlin_script)
        r = requests.get(url)
        if r.content:
            content = simplejson.loads(r.content)['results']
        if r.error:
            raise RexsterException(content['message'])
        elif content:
            return content

    # Rexster v2.2.0-SNAPSHOT tested !
    def shortest_path(self, start, end):
        if type(start) != Vertex or type(end) != Vertex:
            raise RexsterException("both start and end must be valid vertices!")

        gremlin_script = '(new edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath(new GraphJung(g))).getPath(g.v(%d),g.v(%d))' % (start.getId(), end.getId())
        gremlin_result = self.gremlin_execute(gremlin_script)

        for edge in gremlin_result:
            yield Edge(self, edge.get('_id'))

# Rexster v2.2.0-SNAPSHOT tested !
class Index(object):
    """An class containing all the methods needed by an
    Index object"""

    # Rexster v2.2.0-SNAPSHOT tested !
    def __init__(self, graph, indexName, indexClass):
        self.graph = graph
        self.indexName = indexName
        short_indexClass = (indexClass.split('.')[-1]).lower()
        if 'vertex' in short_indexClass:
          self.indexClass = 'vertex'
        elif 'edge' in short_indexClass:
          self.indexClass = 'edge'
        else:
          raise RexsterException("Cannot determine indexClass for %s" % indexClass)
        self.url = "%s/indices/%s" % (self.graph.url, self.indexName)

    # Rexster v2.2.0-SNAPSHOT tested !
    def count(self, key, value):
        """Returns the number of elements indexed for a
        given key-value pair
        @params key: Index key string
        @params outVertex: Index value string

        @returns The number of elements indexed"""
        url = "%s/count?key=%s&value=%s" % (self.url, key, value)
        r = requests.get(url)
        content = simplejson.loads(r.content)
        if r.error:
            raise RexsterException(content['message'])
        return content['totalSize']

    # Rexster v2.2.0-SNAPSHOT tested !
    def getIndexName(self):
        """Returns the name of the index

        @returns The name of the index"""
        return self.indexName

    # Rexster v2.2.0-SNAPSHOT tested !
    def getIndexClass(self):
        """Returns the index class (VERTICES or EDGES)

        @returns The index class"""
        return self.indexClass

    # Rexster v2.2.0-SNAPSHOT tested !
    def put(self, key, value, element):
        """Puts an element in an index under a given
        key-value pair
        @params key: Index key string
        @params value: Index value string
        @params element: Vertex or Edge element to be indexed"""
        if isinstance(element, Vertex):
            klass = 'vertex'
        elif isinstance(element, Edge):
            klass = 'edge'
        else:
            raise RexsterException("Unknown element type")        
        urlfrag = '?key=%s&value=%s&id=%d' % (key, value, element.getId())
        r = requests.put(self.url + urlfrag)
        if r.error:
            error_msg = simplejson.loads(r.content)['message']
            raise RexsterException(error_msg)

    # Rexster v2.2.0-SNAPSHOT tested !
    def get(self, key, value):
        """Gets an element from an index under a given
        key-value pair
        @params key: Index key string
        @params value: Index value string
        @returns A generator of Vertex or Edge objects"""
        urlfrag = '?key=%s&value=%s' % (key, value)
        r = requests.get(self.url + urlfrag)
        content = simplejson.loads(r.content)
        if r.error:
            raise RexsterException(content['message'])
        for item in content['results']:
            if self.indexClass in ('vertex', 'neo4jvertex'):
                yield Vertex(self.graph, item.get('_id'))
            else:
                yield Edge(self.graph, item.get('_id'))

    # Rexster v2.2.0-SNAPSHOT tested !
    def remove(self, key, value, element):
        """Removes an element from an index under a given
        key-value pair
        @params key: Index key string
        @params value: Index value string
        @params element: Vertex or Edge element to be removed"""
        if isinstance(element, Vertex):
            klass = 'vertex'
        elif isinstance(element, Edge):
            klass = 'edge'
        else:
            raise RexsterException("Unknown element to be deleted")
        _id = element.getId()
        urlfrag = '?key=%s&value=%s&class=%s&id=%d' % (key, value, self.indexClass, _id)
        r = requests.delete(self.url + urlfrag)
        if r.error:
            raise RexsterException("Could not delete element")

    # Rexster v2.2.0-SNAPSHOT tested !
    def __str__(self):
        return "Index (%s, %s)" % (self.indexName, self.indexClass)

# Rexster v2.2.0-SNAPSHOT tested !
class RexsterIndexableGraph(RexsterGraph):
    """An class containing the specific methods
    for indexable graphs"""   

    # Rexster v2.2.0-SNAPSHOT tested !
    def createManualIndex(self, indexName, indexClass):
        """Creates a manual index
        @params name: The index name
        @params indexClass: vertex or edge

        @returns The created Index"""

        indexClass = indexClass.lower()
        if indexClass != 'vertex' and indexClass != 'edge':
          raise RexsterException("%s is not a valid indexClass" % indexClass)

        url = "%s/indices/%s?class=%s" % (self.url, indexName, indexClass)
        r = requests.post(url)
        content = simplejson.loads(r.content)
        if r.error:
            raise RexsterException(content['message'])
        content = content['results']
        return Index(self, content['name'], content['class'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def createAutomaticIndex(self, indexName, indexClass, autoKey):
        """Creates an automatic index
        @params name: The index name
        @params indexClass: vertex or edge
        @params autoKey: The automatically indexed property"""

        indexClass = indexClass.lower()
        if indexClass != 'vertex' and indexClass != 'edge':
            raise RexsterException("%s is not a valid indexClass" % indexClass)
        url = "%s/keyindices/%s/%s" % (self.url, indexClass, autoKey)
        r = requests.post(url)
        if r.error:          
          raise RexsterException('Key Index create error !')

    # Rexster v2.2.0-SNAPSHOT tested !
    def getManualIndices(self):
        """Returns a generator function over all the existing indexes

        @returns A generator function over all rhe Index objects"""
        url = "%s/indices" % self.url
        r = requests.get(url)
        content = simplejson.loads(r.content)
        if r.error:
            raise RexsterException(content['message'])
        for index in content['results']:
            yield Index(self, index['name'], index['class'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def getManualIndex(self, indexName, indexClass=None):
        """Retrieves an index with a given index name and class
        @params indexName: The index name
        @params indexClass: VERTICES or EDGES

        @return The Index object or None"""
        url = "%s/indices/%s" % (self.url, indexName)
        r = requests.get(url)
        content = simplejson.loads(r.content)['results']
        if r.error:
            return None
        return Index(self, content['name'], content['class'])

    # Rexster v2.2.0-SNAPSHOT tested !
    def dropManualIndex(self, indexName):
        """Removes an index with a given indexName
        @params indexName: The index name"""
        url = "%s/indices/%s" % (self.url, indexName)
        r = requests.delete(url)
        if r.error:
            content = simplejson.loads(r.content)
            raise RexsterException(content['message'])
