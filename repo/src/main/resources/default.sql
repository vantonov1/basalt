CREATE INDEX bst_node_class ON bst_node (class);
CREATE INDEX bst_node_version ON bst_node (version);

CREATE INDEX bst_props_name ON bst_props (name, value_s);
CREATE INDEX bst_props_value_s ON bst_props (value_s);
CREATE INDEX bst_props_value_n ON bst_props (value_n);

CREATE INDEX bst_assoc_type ON bst_assoc (type);
CREATE INDEX bst_assoc_name ON bst_assoc (name);
CREATE INDEX bst_assoc_source ON bst_assoc (source, target);

CREATE INDEX bst_aces_acl_authority_id ON bst_aces (authority_id);

