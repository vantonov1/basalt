package com.github.vantonov1.basalt.repo;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Repository contains nodes. Every node has unique id, assigned during creation. Also, node could have type (to filter during search), primary parent (to support cascade deletion) and a number of named properties. Node could be marked as version (historical record), such nodes are skipped during search
 * <p>Repository automatically updates timestamp of last modification</p>
 * <p>Properties names and values have limits on length, defined by schema</p>
 */
public class Node implements Cloneable {
    public String id;
    public String type;
    public String parent;
    public Date modified;
    public Boolean version;
    private Map<String, Serializable> properties;

    public Node() {
    }

    public Node(String id) {
        this.id = id;
    }

    public Node(String type, Map<String, Serializable> properties) {
        this.type = type;
        this.properties = properties;
    }

    public Node(String id, String type, Map<String, Serializable> properties) {
        this.id = id;
        this.type = type;
        this.properties = properties;
    }

    public Node(Node node) {
        this.type = node.type;
        this.id = node.id;
        this.parent = node.parent;
        this.modified = node.modified;
        this.version = node.version;
        this.properties = node.properties;
    }

    public Map<String, Serializable> getProperties() {
        if (properties == null) {
            properties = new HashMap<>();
        }
        return properties;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T get(String name) {
        return hasProperties() ? (T) getProperties().get(name) : null;
    }

    public void put(String name, Serializable value) {
        getProperties().put(name, value);
    }

    public void add(String propName, Serializable value) {
        if (propName != null && value != null) {
            put(propName, value);
        }
    }

    public boolean hasProperties() {
        return properties != null && !properties.isEmpty();
    }

    public void remove(String name) {
        if(properties != null) {
            properties.remove(name);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object clone() throws CloneNotSupportedException {
        final Node result = (Node) super.clone();
        result.type = type;
        result.id = id;
        result.parent = parent;
        result.modified = modified;
        result.version = version;
        if (properties instanceof HashMap) {
            result.properties = (Map<String, Serializable>) ((HashMap<String, Serializable>) properties).clone();
        } else {
            result.properties = properties;
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return  Objects.equals(id,  node.id)
        && Objects.equals(modified,  node.modified)
        && Objects.equals(parent,  node.parent)
        && Objects.equals(properties,  node.properties)
        && Objects.equals(type,  node.type)
        && Objects.equals(version,  node.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, modified, parent, version);
    }
}

