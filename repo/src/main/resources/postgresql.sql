CREATE INDEX bst_node_class ON bst_node (parent_id, id);
CREATE INDEX bst_node_version ON bst_node (version, class, id);

CREATE INDEX bst_props_node_id ON bst_props (node_id, name);
CREATE INDEX bst_props_name ON bst_props (name, value_s varchar_pattern_ops, node_id);
CREATE INDEX bst_props_value_s ON bst_props (value_s varchar_pattern_ops, node_id);
CREATE INDEX bst_props_value_n ON bst_props (value_n, node_id);

CREATE INDEX bst_assoc_type ON bst_assoc (type);
CREATE INDEX bst_assoc_name ON bst_assoc (name);
CREATE INDEX bst_assoc_source ON bst_assoc (source, target);
CREATE INDEX bst_assoc_target ON bst_assoc (target);

CREATE INDEX bst_aces_acl_node_id ON bst_aces (node_id, authority_id, mask);
CREATE INDEX bst_aces_acl_authority_id ON bst_aces (authority_id, node_id);