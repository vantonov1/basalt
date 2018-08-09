ALTER TABLE bst_props ALTER COLUMN value NVARCHAR(4000);

CREATE INDEX bst_node_class ON bst_node (parent_id) INCLUDE(id);
CREATE INDEX bst_node_version ON bst_node (version, class) INCLUDE(id);

CREATE INDEX bst_props_node_id ON bst_props (node_id, name);
CREATE INDEX bst_props_name ON bst_props (name) INCLUDE(node_id);
CREATE INDEX bst_props_value_s ON bst_props (value_s) INCLUDE(node_id);
CREATE INDEX bst_props_value_n ON bst_props (value_n) INCLUDE(node_id);

CREATE INDEX bst_assoc_type ON bst_assoc (type);
CREATE INDEX bst_assoc_name ON bst_assoc (name);
CREATE INDEX bst_assoc_source ON bst_assoc (source, target);
CREATE INDEX bst_assoc_target ON bst_assoc (target);

CREATE INDEX bst_aces_acl_node_id ON bst_aces (node_id) INCLUDE(authority_id, mask);
CREATE INDEX bst_aces_acl_authority_id ON bst_aces (authority_id) INCLUDE(node_id);

COMMIT TRANSACTION