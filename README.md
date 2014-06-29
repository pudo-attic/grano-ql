# Grano Query Language

This repository contains an experimental query language implementation for grano. It is
intended to augment the existing REST API with a more advanced way of accessing data.

Grano QL is inspired by MQL, the [Metaweb Query Language](http://wiki.freebase.com/wiki/MQL)
used to query Freebase. Its query-by-example approach seems more appropriate for a web
interface than SQL-inspired query languages such as Neo4J's [CYPHER](http://docs.neo4j.org/chunked/stable/cypher-query-lang.html)
or RDF's [SPARQL](http://www.w3.org/TR/rdf-sparql-query/).

* [MQL language reference](http://mql.freebaseapps.com/ch03.html)
* [MQL operators](http://wiki.freebase.com/wiki/MQL_operators)

## Comments and Feedback

This document and the implementation in this repository are requests for
comments - none of the features are fixed at this point; and any
concerns by users are valuable feedback, even if they imply significant
changes to the language. 

## Basic queries

A simple query could look like this:

    {
      properties: {
        name: "Barack Obama"
      },
      id: null
    }

Which means: *get the id of the entity with the name Barack Obama.*

What's cool about this type of query:

* Readable JSON, easily constructed by a web frontend application. 
* Resembles the representation in the REST API; query and result are basically the same thing.
* Granular access to individual properties, or constellations of objects.

Potential Problems:

* How do we tell the difference between null as in "return this value" and null as in "this property is null"?

## Running queries

When installed the Grano QL API endpoint is available at:

	/api/1/query

Queries can be submitted via HTTP GET or POST request. For GET requests, a JSON string is expected to be submitted in the ``query`` query string argument. POST requests are expected to carry the payload as the body, using ``application/json`` as a content type.


