package com.github.vantonov1.basalt.repo;

/**
 * Information about association between {@link Node}, returned by {@link NodeService}<br>
 * Associations are close to UML - they are directional (have source and target), have type (classifier) and optionally name (role)
 */
public class Assoc {
    public final String type;
    public final String name;
    public final String source;
    public final String target;

    public Assoc(String type, String name, String source, String target) {
        assert type != null;
        this.type = type;
        this.name = name;
        this.source = source;
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Assoc assoc = (Assoc) o;

        return type.equals(assoc.type) && name.equals(assoc.name) && source.equals(assoc.source) && target.equals(assoc.target);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }
}

