package com.github.vantonov1.basalt.acl;

import com.github.vantonov1.basalt.cache.TransactionalCacheManager;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import com.github.vantonov1.basalt.repo.SearchService;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Manages ACLs (Access Control Lists) attached to {@link Node Nodes}.
 * Every ACL consists of mapping between authority GUID (authorities could be represented as nodes in repository or external entities, in Active Directory for example) and access mask - set of rights as bitmap.
 * Standard rights are MASK_READ(1), MASK_WRITE(2) and MASK_DELETE(4), but you could add your own bits
 * <p>AUTHORITY_WORLD is used to represent access for everyone (it is well-known authority in Windows)</p>
 * <p>Set of methods to check access rights takes two parameters - authority itself and list of its memberships (GUIDS of authority groups etc).
 * Access is given if authority  or any group has it, in line with common "persons and groups" system</p>
 * <p>If there is no access mask for the given authority on the node, primary parents hierarchy will be checked, until parent ACL contains item for the authority or membership groups, or there is no primary parent</p>
 * <p>ACLs are cached using {@link TransactionalCacheManager TransactionalCacheManager}</p>
 */
public interface AclService {
    int MASK_READ = 1;
    int MASK_WRITE = 2;
    int MASK_DELETE = 4;

    String AUTHORITY_WORLD = "S0000000-0001-0001-0000-000000000000";
    Map<String, Integer> EVERYONE_READ_WRITE = Collections.singletonMap(AUTHORITY_WORLD, MASK_READ | MASK_WRITE);
    Map<String, Integer> EVERYONE_READ = Collections.singletonMap(AUTHORITY_WORLD, MASK_READ);

    /**
     * Add ACL to existing node (keeps all already existing items). Duplication of items for one authority is not checked, but it is logical error to have to items for authority - order is not defined.
     * Please use {@link #getAcl} to join ACLs
     * @param id node GUID
     * @param acl mapping betweena  authorities and access masks
     */
    void addAcl(@NonNull String id, @NonNull Map<String, Integer> acl);

    /**
     * Replace or create ACL
     * @param id node GUID
     * @param acl mapping betweena  authorities and access masks
     */
    void setAcl(@NonNull String id, @Nullable Map<String, Integer> acl);

    /**
     * Replace or create ACL for the given authority.
     * @param id node GUID
     * @param authority GUID
     * @param mask could be null, means no access for the given authority
     */
    void setAcl(@NonNull String id, @NonNull String authority, @Nullable Integer mask);

    /**
     * Remove all ACLs for the given authority. Should be called if authority is removed
     * @param authority GUID
     */
    void removeACLs(@NonNull String authority);

    /**
     * Get ACL for the node. If node has no ACL set, primary parents hierarchy will be checked
     * @param id node GUID
     * @return ACL
     */
    @Nullable Map<String, Integer> getAcl(@Nullable String id);

    /**
     * Converts existing ACL for the node to MASK_READ, or set it to EVERYONE_READ if there were no ACL
     * @param id node GUID
     * @return true if node had ACL
     */
    boolean convertToReadonly(@NonNull String id);

    /**
     * Converts existing ACL for the node to MASK_READ | MASK_WRITE, or set it to EVERYONE_READ_WRITE if there were no ACL
     * @param id node GUID
     * @return true if node had ACL
     */
    boolean convertToReadWrite(@NonNull String id);

    /**
     * Checks if authority or any of it membership groups have MASK_READ access to node.
     * <p>If there is no access mask for the given authority on the node, primary parents hierarchy will be checked, until parent ACL contains item for the authority or membership groups, or there is no primary parent</p>
     * @param id node GUID
     * @param authority GUID
     * @param membership collection of containing authority GUIDs
     * @return true if access is given
     */
    boolean isReadableBy(@Nullable String id, @Nullable String authority, @Nullable Collection<String> membership);

    /**
     * Checks if authority or any of it membership groups have MASK_WRITE access to node.
     * <p>If there is no access mask for the given authority on the node, primary parents hierarchy will be checked, until parent ACL contains item for the authority or membership groups, or there is no primary parent</p>
     * @param id node GUID
     * @param authority GUID
     * @param membership collection of containing authority GUIDs
     * @return true if access is given
     */
    boolean isWritableBy(@Nullable String id, @Nullable String authority, @Nullable Collection<String> membership);

    /**
     * Checks if authority or any of it membership groups have MASK_DELETE access to node.
     * <p>If there is no access mask for the given authority on the node, primary parents hierarchy will be checked, until parent ACL contains item for the authority or membership groups, or there is no primary parent</p>
     * @param id node GUID
     * @param authority GUID
     * @param membership collection of containing authority GUIDs
     * @return true if access is given
     */
    boolean isDeletableBy(@Nullable String id, @Nullable String authority, @Nullable Collection<String> membership);

    /**
     * Filters collection of node ids to get only those visible to the authority.
     * Intended to be used with {@link SearchService#search SearchService.search()} or {@link NodeService#getChildAssoc NodeService.getChildAssoc()}
     * to return only nodes visible to the current authority
     * <p>Method uses bulk loading of ACLs and primary parents (using {@link NodeService#getPrimaryParents NodeService.getPrimaryParents()}) and is much faster on large collections than checking access one-by-one using {@link #isReadableBy}</p>
     * @param ids nodes GUIDs
     * @param authority GUID
     * @param membership collection of containing authority GUIDs
     * @return nodes GUIDs visible to the given authority
     */
    @NonNull List<String> filterReadable(@Nullable Collection<String> ids, @Nullable String authority, @Nullable Collection<String> membership);
}
