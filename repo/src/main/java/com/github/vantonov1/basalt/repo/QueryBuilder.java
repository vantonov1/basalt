package com.github.vantonov1.basalt.repo;

import com.github.vantonov1.basalt.repo.impl.TypeConverter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Constructs query for {@link SearchService}. General pattern is
 * <pre>
 * statement1 AND statement2 AND ...
 *  AND oneof()
 * statement3 OR statement4 OR ...
 * </pre>
 * Example - search png or jpeg photo where name like 'bird%':
 * <pre>
 * new QueryBuilder()
 *  .type(Arrays.asList("photo"))
 *  .is("name", "bird%")
 *  .oneOf()
 *  .is("content-type", "image/jpeg")
 *  .is("content-type", "image/png")
 * </pre>
 * Both parts could contain parenthesized groups of statements, so it would be like that:
 * <pre>
 * statement1 AND (statement2 OR statement3...) AND ...
 * oneOf()
 * statement4 OR (statement5 AND statement6 ...) OR ...
 * </pre>
 */
public class QueryBuilder {
    private static final String OPERATOR_DIFFERENT = "<>";
    private static final String OPERATOR_NOT = "not ";
    private static final String OPERATOR_LTE = "<=";
    private static final String OPERATOR_GTE = ">=";
    private static final String OPERATOR_EQUALS = "=";
    private static final String OPERATOR_LIKE = " like ";
    private static final String QUERY_FILTER_AND = " and ";
    private static final String QUERY_FILTER_OR = " or ";

    private final StringBuilder query = new StringBuilder(512);

    private StringBuilder result;
    private boolean or;
    private boolean group;
    private int joinCount = 0;
    private boolean hasAnd;
    private boolean hasOr;
    private boolean hasParentAssoc;
    private boolean hasChildAssoc;

    /**
     * Builds resulting query string. Query could not be changed after that call. Called by {@link SearchService}
     *
     * @return SQL expression
     */
    public String build() {
        if (result == null) {
            result = new StringBuilder(512);
            for (int i = 0; i < joinCount; i++) {
                result.append("join bst_props p").append(i).append(" on n.id = p").append(i).append(".node_id ");
            }
            if (hasParentAssoc) {
                result.append("join bst_assoc pa on pa.target = n.id ");
            }
            if (hasChildAssoc) {
                result.append("join bst_assoc ca on ca.source = n.id ");
            }
            if (group) {
                query.append(')');
            }
            if (or) {
                query.append(')');
            }
            final String q = query.toString();
            if (!q.isEmpty()) {
                result.append(" where ");
                result.append(q);
            }
        }
        return result.toString();
    }

    /**
     * Type filter. Generate <code>type IN(...)</code> or type = <code>?</code>
     */
    public QueryBuilder type(@NonNull String... types) {
        return type(Arrays.asList(types));
    }

    /**
     * Type filter. Generate <code>type IN(...)</code> or type = <code>?</code>
     */
    public QueryBuilder type(Collection<String> types) {
        if (!isEmpty(types)) {
            andOr();
            if (types.size() == 1) {
                query.append("n.class='").append(types.iterator().next()).append("' ");
            } else {
                query.append("n.class in ('").append(join(types, "','")).append("') ");
            }
        }
        return this;
    }

    /**
     * Filter by value in named property. Generate <code>name=? AND value=?</code>, <code>value LIKE ?</code> or <code>value IS NULL</code>, depending on value
     */
    public QueryBuilder is(@Nullable String name, @Nullable Object value) {
        if (value != null) {
            andOr();
            if (value instanceof String && ((String) value).indexOf('*') != -1) {
                fillOperator(name, ((String) value).replace('*', '%'), OPERATOR_LIKE);
            } else {
                fillOperator(name, value, OPERATOR_EQUALS);
            }
            joinCount++;
            return this;
        } else {
            return isNull(name);
        }
    }

    /**
     * Filter by value in named property. Generate <code>name=? AND value!=?</code>, <code>value NOT LIKE ?</code> or <code>value IS NOT NULL</code>, depending on value
     */
    public QueryBuilder isNot(String name, Object value) {
        if (value != null) {
            andOr();
            if (value instanceof String && ((String) value).indexOf('*') != -1) {
                fillOperator(name, ((String) value).replace('*', '%'), ' ' + OPERATOR_NOT + OPERATOR_LIKE);
            } else {
                fillOperator(name, value, OPERATOR_DIFFERENT);
            }
            joinCount++;
            return this;
        } else {
            return isNotNull(name);
        }
    }

    /**
     * Nested query for name and value. Generate <code>NOT IN (select id where name = ? and (value = ? or ...))</code>
     */
    public QueryBuilder isNot(String name, List<Object> values) {
        if (values != null && !values.isEmpty()) {
            andOr();
            query.append("n.id not in (select node_id from bst_props where ");
            fillPropName(name, "bst_props");
            query.append("(");
            int i = 0;
            for (Object v : values) {
                fillValue(v, OPERATOR_EQUALS, "bst_props");
                if (++i < values.size()) {
                    query.append(QUERY_FILTER_OR);
                }
            }
            query.append("))");
        }
        return this;
    }

    /**
     * Filter by value is null in named property. Generate <code>name=? AND value IS NULL</code>
     */
    public QueryBuilder isNull(String... names) {
        if (names.length > 0) {
            if (or) {
                throw new IllegalStateException("or is null is not supported (yet?)");
            }
            and();
            query.append("n.id not in (select node_id from bst_props where ");
            int i = 0;
            for (String n : names) {
                query.append("(");
                fillPropName(n, "bst_props");
                query.append(")");
                if (++i < names.length) {
                    query.append(QUERY_FILTER_OR);
                }
            }
            query.append(")");
        }
        return this;
    }

    /**
     * Filter by value is not null in named property. Generate <code>name=? OR ...</code> (repository does not store null values, so that is enough)<br>
     * <b>Could not be used after oneOf()</b>
     */
    public QueryBuilder isNotNull(String... names) {
        return isNotNull(Arrays.asList(names));
    }

    /**
     * Filter by value is not null in named property. Generate <code>name=? OR ...</code> (repository does not store null values, so that is enough)<br>
     * <b>Could not be used after oneOf()</b>
     */
    public QueryBuilder isNotNull(Collection<String> names) {
        if (!names.isEmpty()) {
            if (or) {
                throw new IllegalStateException("or is not null is not supported (yet?)");
            }
            and();
            query.append(" (");
            int i = 0;
            for (String name : names) {
                fillPropName(name);
                if (i++ < names.size() - 1) {
                    query.append(QUERY_FILTER_OR);
                }
            }
            query.append(")");
            joinCount++;
        }
        return this;
    }

    /**
     * Filter by primary parent. Generate <code>parent=?</code>
     */
    public QueryBuilder primaryParent(String parentId) {
        if (parentId != null) {
            andOr();
            query.append("n.parent=").append(parentId);
        }
        return this;
    }

    /**
     * Filter by value is in range in named property. Generate <code>name=? AND value &gt;= ? AND value &lt;= ?</code><br>
     * Min or max value could be skipped
     */
    public QueryBuilder range(String name, Object min, Object max) {
        andOr();
        query.append(" (");
        fillPropName(name);
        query.append(QUERY_FILTER_AND);

        if (min != null) {
            if (max != null) {
                fillValue(min, OPERATOR_GTE);
                query.append(QUERY_FILTER_AND);
                fillValue(max, OPERATOR_LTE);
            } else {
                fillValue(min, OPERATOR_GTE);
            }
        } else {
            assert max != null;
            fillValue(max, OPERATOR_LTE);
        }
        query.append(")");
        joinCount++;
        return this;
    }

    /**
     * Filter by having parent association with nodes. Joins with associations and generate <code>source=? AND ...</code>
     */
    public QueryBuilder parentAssoc(String... parents) {
        andOr();
        if (parents.length == 1) {
            query.append("pa.source='").append(parents[0]).append('\'');
        } else {
            query.append("pa.source='").append(join(Arrays.asList(parents), "' and pa.source'")).append('\'');
        }
        hasParentAssoc = true;
        return this;
    }

    /**
     * Nested query for not having parent associations with nodes. Generate <code>NOT IN (select target WHERE source IN (...))</code>
     */
    public QueryBuilder parentAssocNot(String... parents) {
        andOr();
        query.append(" n.id not in (select target from bst_assoc where ");
        if (parents.length == 1) {
            query.append("source='").append(parents[0]).append('\'');
        } else {
            query.append("source in ('").append(join(Arrays.asList(parents), "','")).append(')');
        }
        query.append(')');
        return this;
    }

    /**
     * Filter by having child association with nodes. Joins with associations and generate <code>target=? AND ...</code>
     */
    public QueryBuilder childAssoc(String... children) {
        andOr();
        if (children.length == 1) {
            query.append("ca.target='").append(children[0]).append('\'');
        } else {
            query.append("ca.target='").append(join(Arrays.asList(children), "' and ca.target'")).append('\'');
        }
        hasChildAssoc = true;
        return this;
    }

    /**
     * Nested query for not having child associations with nodes. Generate <code>NOT IN (select source WHERE target IN (...))</code>
     */
    public QueryBuilder childAssocNot(String... parents) {
        andOr();
        query.append(" n.id not in (select source from bst_assoc where ");
        if (parents.length == 1) {
            query.append("target='").append(parents[0]).append('\'');
        } else {
            query.append("target in ('").append(join(Arrays.asList(parents), "','")).append(')');
        }
        query.append(')');
        return this;
    }

    /**
     * Joins nodes by value in named property. Generate <code>name=&lt;name&gt; and value IN (select id where name=&lt;referencedName&gt; and value=&lt;referencedValue&gt;</code>
     */
    public QueryBuilder reference(String name, String referencedName, Object referencedValue) {
        andOr();
        query.append(" (");
        fillPropName(name);
        query.append(QUERY_FILTER_AND);
        query.append('p').append(joinCount).append(".value_s in (select node_id from bst_props where name=").append(referencedName).append(" and value_s");
        if (referencedValue instanceof String && ((String) referencedValue).indexOf('*') != -1) {
            query.append(OPERATOR_LIKE).append(((String) referencedValue).replace('*', '%'));
        } else {
            query.append(OPERATOR_EQUALS).append(referencedValue);
        }
        query.append("))");
        joinCount++;
        return this;
    }

    /**
     * Starts second part of the query, where properties should match at least one of expressions
     */
    public QueryBuilder oneOf() {
        if (or) {
            throw new IllegalStateException("oneOf() can't be called twice");
        }
        or = true;
        if (group) {
            query.append(')');
            group = false;
        }
        and();
        query.append('(');
        hasOr = false;
        hasAnd = false;
        return this;
    }

    /**
     * Starts parenthesized group of expressions. Before oneOf() it generates <code>(statement1 AND (statement2 OR statement3...) AND ...)</code>, after (statement4 OR (statement5 AND statement6 ...) OR ...)<br>
     * Group finishes if another group started, or oneOf() encountered, or the whole query is finished
     */
    public QueryBuilder group() {
        if (or) {
            if (group) {
                query.append(')');
                query.append(QUERY_FILTER_OR);
            } else {
                or();
            }
        } else {
            if (group) {
                query.append(')');
                query.append(QUERY_FILTER_AND);
            } else {
                and();
            }
        }
        group = true;
        hasOr = false;
        hasAnd = false;
        query.append('(');
        return this;
    }

    /**
     * Utility method to check, if query contains expressions
     */
    public boolean isEmpty() {
        return query.toString().isEmpty();
    }

    private void or() {
        if (hasOr) {
            query.append(QUERY_FILTER_OR);
        }
        hasOr = true;
    }

    private void and() {
        if (hasAnd) {
            query.append(QUERY_FILTER_AND);
        }
        hasAnd = true;
    }

    private void andOr() {
        if ((or || group) && !(or && group)) {
            or();
        } else {
            and();
        }
    }

    private void fillOperator(String propName, Object propValue, String op) {
        hasAnd = true;
        query.append(" (");
        if (propName != null) {
            fillPropName(propName);
            query.append(QUERY_FILTER_AND);
        }
        fillValue(propValue, op);
        query.append(")");
    }

    private void fillValue(Object propValue, String op) {
        fillValue(propValue, op, "p" + joinCount);
    }

    private void fillValue(Object propValue, String op, String tableName) {
        final String s = TypeConverter.getString(propValue);
        final Long n = TypeConverter.getNumeric(propValue);
        query.append(tableName);
        if (s != null) {
            query.append(".value_s ").append(op);
            query.append(" '").append(safeTemplate(s)).append("'");
        } else if (n != null) {
            query.append(".value_n ").append(op);
            query.append(" '").append(n).append("'");
        } else {
            query.append(".value_s is null and ").append(tableName).append(".value_n is null");
        }
    }

    private void fillPropName(String propName) {
        fillPropName(propName, "p" + joinCount);
    }

    private void fillPropName(String propName, String tableName) {
        query
                .append(tableName)
                .append(".name = '")
                .append(propName)
                .append("'");

    }

    private boolean isEmpty(Collection list) {
        return list == null || list.isEmpty();
    }

    private static String safeTemplate(String template) {
        return template.replaceAll("\\\\", "").replaceAll("\"", "\\\\\"");
    }

    private static String join(Collection<String> elements, String delimiter) {
        return elements.stream().collect(Collectors.joining(delimiter));
    }
}
