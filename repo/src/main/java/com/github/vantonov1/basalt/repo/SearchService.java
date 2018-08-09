package com.github.vantonov1.basalt.repo;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Searches nodes in repository by types, properties and associations between nodes
 * @see QueryBuilder
 */
public interface SearchService {
    /**
     * Search using node type and property name/value. Resulting query is<br>
     *  <i>node.type IN (types) AND property.name = propName AND property.value = propValue</i><br>
     *  Wildcard (%) could be used to search values using LIKE
     * @return list of nodes GUIDs
     */
    @Nullable List<String> search(@Nullable Collection<String> types, @Nullable String propName, @Nullable Serializable propValue);

    /**
     * Search by expression built from {@link QueryBuilder}
     * @param q expression
     * @return list of nodes GUIDs
     */
    @Nullable List<String> search(@NonNull QueryBuilder q);

    /**
     * Search by expression built from {@link QueryBuilder}, additionally filtered by primary parents
     * @param q expression
     * @param primaryParents list of parents GUIDs. Query is splitted by batches, 1000 of GUIDs per batch
     * @param limit max number of rows. Not used if &lt;= 0
     * @return list of nodes GUIDs
     */
    @Nullable List<String> search(@NonNull QueryBuilder q, @Nullable Collection<String> primaryParents, int limit);

    /**
     * Count by expression. In some DBMS, <i>select count(distinct id)</i> could be ineffective
     * @param q expession
     * @return nodes count
     */
    int count(@NonNull QueryBuilder q);
}
