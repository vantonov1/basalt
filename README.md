# Basalt
If JPA is too constraining and heavy for your data model, but document-oriented NoSQL databases lacks good transaction support - Basalt is for you.

It was developed as a lightweight, much faster replacement for the [Alfresco ECM](http://alfresco.com) repository services.

Basalt creates document-oriented repository on top of usual relational DBMS, attaches unstructured content to the nodes, manages access rights, and supports full-text indexing and searching.

Like Alfresco, it works with nodes and associations, but is NOT fully graph-oriented - only one level of source/target associations could be retrieved at one call.
Unlike Alfresco, where almost all document properties are duplicated in Solr, Basalt uses search engines only for full-text search, and constructs SQL for usual properties.

Basalt uses Spring and is Spring Boot ready, just add it to the dependencies, and set some system properties. It expects usual Spring infrastructure for working with database, like DataSource, PlatformTransactionManager and CacheManager.

Basalt does not manage transactions, but requires them. It uses optimistic locking to achieve consistency, so be ready to handle TransientDataAccessException appropriately (RetryingTransactionHelper could be used).

Basalt was tested on MySQL, PostgresSQL, MS SQL and HSQLDB. Most probably it will work on other relational databases, it uses simple SQL, but indexing schema could be inefficient, and there always subtle differences...

## Getting started
- Add basalt-all to the dependencies (Or you could select modules needed, see [More info](https://github.com/vantonov1/basalt#more-info)). It is on the Maven Central, so for Maven just add

```xml
        <dependency>
            <groupId>com.github.vantonov1</groupId>
            <artifactId>basalt-all</artifactId>
            <version>1.0</version>
        </dependency>
```

- Configure database. Lets use in-memory database HSqlDB - just add org.hsqldb:hsqldb to dependencies, Spring Boot
        automatically creates DataSource
- We are going to store and index content in memory, so add two rows to application.properties (in real code, set them to paths on the local filesystem)
    
```
content.root=mem
lucene.root=mem
```
        
- Inject NodeService, SearchService and ContentService into your bean
- Start a transaction using Spring. You have to deal with optimistic locking, and retry transaction in case of
        collisions. Lets use RetryingTransactionHelper provided by Basalt:
```java
 @Autowired PlatformTransactionManager transactionManager; //Spring Boot will auto-configure DataSourceTransactionManager
 ...
 return new RetryingTransactionHelper(transactionManager).doInTransaction(false, () -> {
  //your code
  return result;
});
 ```

- Lets add file with "Lorem ipsum" (of course) as repository node
```java
Node node = new Node("citation", Collections.singletonMap("language", "latin")); // new node with type and properties
String id = nodeService.createNode(null, node, null, null); // just created node without primary parent association
contentService.putContent(id, new File("lorem_ipsum.txt")); //attached content from the file
```

Ok, we did that. Now get it back

```java
Node node = nodeService.getProperties(id);
assert "citation".equals(node.type);
assert "latin".equals(node.get("language"));

Content c = contentService.getContent(id);
Reader r = new InputStreamReader(c.stream, Charset.forName(c.encoding));
```

We could find all citations in latin:
```java
List<String> ids = searchService.search(Collections.singleton("citation"), "language", "latin");
```

Or use `QueryBuilder` for more advanced searches:

```java
ids = searchService.search(new QueryBuilder()
  .type("citation")
  .is("language", "latin"));
```

### More Info
 Basalt consists of 4 modules. Please check javadoc's for more information, starting with the following services:
 
 - basalt-repo - contains `NodeService` to manage nodes and associations between nodes, and `SearchService` for attribute-based search (builds SQL queries to DB)
 - basalt-content - contains `ContentService` to manage unstructured content, attached to nodes
 - basalt-fulltext - contains `FullTextSearchService` to search using full-text search engine (for now Lucene and Solr supported) in content and selected node properties
 - basalt-acl - contains `AclService` to manage ACL's, attached to nodes, and check access rights
