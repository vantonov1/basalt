CREATE TABLE bst_node (
  id CHAR(36) PRIMARY KEY NOT NULL,
  parent_id CHAR(36),
  version CHAR(1),
  modified BIGINT NOT NULL,
  class VARCHAR(255) NOT NULL,
  CONSTRAINT parent FOREIGN KEY (parent_id) REFERENCES bst_node (id) ON DELETE NO ACTION
);

CREATE TABLE bst_props (
  node_id CHAR(36) NOT NULL,
  name VARCHAR(255)NOT NULL,
  type SMALLINT,
  value_s VARCHAR(4096),
  value_n NUMERIC(19),
  CONSTRAINT node FOREIGN KEY (node_id) REFERENCES bst_node (id) ON DELETE CASCADE
);

CREATE TABLE bst_assoc (
  type VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  source CHAR(36) NOT NULL,
  target CHAR(36) NOT NULL,
  CONSTRAINT source FOREIGN KEY (source) REFERENCES bst_node (id) ON DELETE NO ACTION,
  CONSTRAINT target FOREIGN KEY (target) REFERENCES bst_node (id) ON DELETE NO ACTION
);

CREATE TABLE bst_aces (
  node_id CHAR(36) NOT NULL,
  authority_id CHAR(36) NOT NULL,
  mask SMALLINT,
  CONSTRAINT aces_node FOREIGN KEY (node_id) REFERENCES bst_node (id) ON DELETE CASCADE
);