# Grano Query Language

This repository contains an experimental query language implementation for grano. It is
intended to augment the existing REST API with a more advanced way of accessing data.

Grano QL is inspired by MQL, the [Metaweb Query Language](http://wiki.freebase.com/wiki/MQL)
used to query Freebase. Its query-by-example approach seems more appropriate for a web
interface than SQL-inspired query languages such as Neo4J's [CYPHER](http://docs.neo4j.org/chunked/stable/cypher-query-lang.html)
or RDF's [SPARQL](http://www.w3.org/TR/rdf-sparql-query/).

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
      }
    }


