package com.github.vantonov1.basalt.repo;

import com.github.vantonov1.basalt.cache.TransactionalCacheManager;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Supports document-oriented repository with {@link Node} and {@link Assoc} between nodes
 * <p>Optimistic locking is used, so you could get {@link org.springframework.dao.OptimisticLockingFailureException OptimisticLockingFailureException}. Use {@link RetryingTransactionHelper}, or rollback and retry transactions in your code<br>
 * Please be aware, that bulk updates (updateProperties/setProperty for collection of nodes) could not use optimistic locking - it is up to you to avoid race conditions</p>
 * <p>Both nodes and associations are cached using {@link TransactionalCacheManager TransactionalCacheManager} - please do not cache repository by yourself</p>
 */
public interface NodeService {
    /**
     * Creates {@link Node} in repository. Node may have primary {@link Assoc} with parent (node is automatically deleted if parent or parent assoc is deleted)
     * @see NodeService#addChild addChild()
     * @param parentId id of the parent node
     * @param node initial node properties. If node.id is provided, it will be checked for uniqueness, otherwise new GUID is assigned, so it is faster to use id from repository.
     *             Also, pay attention that most DBMS insert sequential GUIDs much faster (repository GUIDs use timestamps to be monotonically increasing)
     * @param assocType like UML classifier for association
     * @param assocName like UML role name for association
     * @return GUID of created node
     */
    @NonNull String createNode(@Nullable String parentId, @NonNull Node node, @Nullable String assocType, @Nullable String assocName);

    /**
     * Bulk nodes creation inside one parent
     * @param parentId id of the parent node
     * @param nodes initial properties
     * @param assocType like UML classifier for association
     * @return GUIDs of created nodes
     */
    @NonNull List<String> createNodes(@Nullable String parentId, @NonNull Collection<Node> nodes, @Nullable String assocType);

    /**
     * Bulk nodes creation inside different parents
     * @param nodesInParent nodes properties, sorted by primary parent
     * @param assocType like UML classifier for association
     * @return GUIDs of created nodes, sorted by primary parents
     */
    @NonNull Map<String, Collection<String>> createNodes(@NonNull Map<String, Collection<Node>> nodesInParent, @Nullable String assocType);

    /**
     * Updates node properties. Node id could not be changed after creation. Node modified timestamp is updated automatically
     * <p>{@link org.springframework.dao.OptimisticLockingFailureException OptimisticLockingFailureException} could be thrown. Use {@link RetryingTransactionHelper}, or rollback and retry transactions in your code</p>
     * @param updated properties
     * @param deleteOld if set, delete all properties except provided, otherwise only add and update
     */
    void updateProperties(@NonNull Node updated, boolean deleteOld);

    /**
     * Bulk update of properties<br>
     * WARNING Method intended to be used in cases like migration etc, and does not use locking
     * @param nodes updated properties
     * @param deleteOld if set, delete all properties except provided, otherwise only add and update
     */
    void updateProperties(@NonNull Collection<Node> nodes, boolean deleteOld);

    /**
     * Set (add, update or remove) node property by name
     * <p>{@link org.springframework.dao.OptimisticLockingFailureException OptimisticLockingFailureException} could be thrown. Use {@link RetryingTransactionHelper}, or rollback and retry transactions in your code</p>
     * @param id node GUID
     * @param name property name
     * @param value new value (remove property if null)
     */
    void setProperty(@NonNull String id, @NonNull String name, Serializable value);

    /**
     * Bulk property set add, update or remove)
     * WARNING Method intended to be used in cases like migration etc, and does not use locking
     * @param ids nodes GUIDs
     * @param name property name
     * @param value new value (remove property if null)
     */
    void setProperty(@NonNull Collection<String> ids, @NonNull String name, Serializable value);

    /**
     * Remove property by name
     * <p>{@link org.springframework.dao.OptimisticLockingFailureException OptimisticLockingFailureException} could be thrown. Use {@link RetryingTransactionHelper}, or rollback and retry transactions in your code</p>
     * @param id node GUID
     * @param name property name
     */
    void removeProperty(@NonNull String id, @NonNull String name);

    /**
     * Get named node property or null
     * @param id node GUID
     * @param name property name
     * @return property value or null
     */
    <T extends Serializable> T getProperty(@Nullable String id, @Nullable String name);

    /**
     * Get nodes named property
     * @param ids node GUIDs
     * @param name property name
     * @return property value by id
     */
    Map<String, Object> getProperty(@Nullable Collection<String> ids, @Nullable String name);

    /**
     * Get node properties
     * @param id node GUID
     * @return properties
     */
    Node getProperties(String id);

    /**
     * Bulk get nodes properties
     * @param ids node GUID
     * @return properties
     */
    List<Node> getProperties(@Nullable Collection<String> ids);

    /**
     * Queries for nodes with specified type and property, and node.version = true. Usual search will skip node versions
     * @param type node type
     * @param propName property name
     * @param propValue property value
     * @return nodes GUIDs
     */
    List<String> getVersions(@Nullable String type, @Nullable String propName, @Nullable Serializable propValue);

    /**
     * Get node primary parent or null
     * @param id node GUID
     * @return parent node GUID
     */
    @Nullable String getPrimaryParent(@Nullable String id);

    /**
     * Bulk get node primary parents
     * @param ids node GUIDs
     * @return parent nodes GUIDs
     */
    @NonNull Map<String, String> getPrimaryParents(@Nullable Collection<String> ids);


    /**
     * Get list of associations where given node is parent, optionally filtered by type and/or name
     * @param parent node GUID
     * @param assocType association type (classifier)
     * @param assocName association name (role)
     * @return list of {@link Assoc}
     */
    @NonNull List<Assoc> getChildAssoc(@Nullable String parent, @Nullable String assocType, @Nullable String assocName);

    /**
     * Get list of associations where given node is child, optionally filtered by type and/or name
     * @param child node GUID
     * @param assocType association type (classifier)
     * @param assocName association name (role)
     * @return list of {@link Assoc}
     */
    @NonNull List<Assoc> getParentAssoc(@Nullable String child, @Nullable String assocType, @Nullable String assocName);

    /**
     * Bulk get list of associations where given nodes are parent, optionally filtered by type and/or name
     * @param ids node GUIDs
     * @param assocType association type (classifier)
     * @param assocName association name (role)
     * @return list of {@link Assoc}
     */
    @NonNull List<Assoc> getChildAssoc(@Nullable Collection<String> ids, @Nullable String assocType, @Nullable String assocName);

    /**
     * Efficient way to check, if there any child nodes presented
     * @param id node GUID
     * @return number of children
     */
    int countAllChildAssocs(@Nullable String id);

    /**
     * Get all associations of specified type. Be carefull, you can easily get tons of results
     * @param type association type (classifier)
     * @return list of {@link Assoc}
     */
    List<Assoc> getAllAssocByType(String type);

    /**
     * Get all associations (both child and parent) of specified node
     * @param id node GUID
     * @return list of {@link Assoc}
     */
    @NonNull List<Assoc> getAllAssoc(@NonNull String id);

    /**
     * Create association between two nodes
     * @param parentId parent node GUID
     * @param childId child node GUID
     * @param assocType like UML classifier for association
     * @param assocName like UML role name for association
     * @param checkIfExists - check if association with specified type already exists between given parent and child. If not set, you can have several associations, probably with different names (roles)
     */
    void addChild(@NonNull String parentId, @NonNull String childId, @NonNull String assocType, @Nullable String assocName, boolean checkIfExists);

    /**
     * Delete node from the repository. Also removes all associations and do cascade removal of all nodes where given node was primary parent
     * @param id node GUID
     */
    void deleteNode(@NonNull String id);

    /**
     * Delete parent-child association. Deletes child node if it was last association with primary parent
     * @param parentId parent node GUID
     * @param childId child node GUID
     * @param assocType like UML classifier for association
     * @param assocName like UML role name for association
     */
    void deleteChild(@NonNull String parentId, @NonNull String childId, @Nullable String assocType, @Nullable String assocName);

    /**
     * Change primary parent of node. If association type and optionally name are provided, association of that type and name with old parent is deleted, and with new parent created
     * @param id node GUID
     * @param to new primary parent GUID
     * @param assocType like UML classifier for association
     * @param assocName like UML role name for association
     */
    void move(@NonNull String id, @Nullable String to, @Nullable String assocType, @Nullable String assocName);

    /**
     * Check if node exists in repository. Faster than getProperties(id) != null;
     * @param id node GUID
     * @return true if id is GUID and node with that id exists in repository
     */
    boolean exists(String id);
}
