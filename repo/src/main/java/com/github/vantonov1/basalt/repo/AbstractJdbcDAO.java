package com.github.vantonov1.basalt.repo;

import com.github.vantonov1.basalt.repo.impl.TypeConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractJdbcDAO {
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final Log logger = LogFactory.getLog(getClass());
    private final JdbcTemplate jdbcTemplate;

    @Value("${db.outstanding.request:100000}")
    private int threshold;

    public AbstractJdbcDAO(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public static List<List<String>> partition(Collection<String> ids) {
        final List<String> list = new ArrayList<>(ids);
        final List<List<String>> result = new ArrayList<>((ids.size() - 1) / DEFAULT_BATCH_SIZE + 1);
        for (int i = 0; i < ids.size(); i += DEFAULT_BATCH_SIZE) {
            result.add(list.subList(i, Math.min(i + DEFAULT_BATCH_SIZE, ids.size())));
        }
        return result;
    }

    protected static void setFetchSize(ResultSet rs, int index) throws SQLException {
        if (index == 0) {
            rs.setFetchSize(50);
        } else if (index == 50) {
            rs.setFetchSize(500);
        } else if (index == 1000) {
            rs.setFetchSize(1000);
        }
    }

    protected int update(String sql, Object... args) throws DataAccessException {
        checkTransaction();
        return log(sql, () -> jdbcTemplate.update(sql, args));
    }

    protected void batchUpdate(String sql, List<Object[]> batch, final int keyIndex) {
        if (!batch.isEmpty()) {
            batch.sort((o1, o2) -> {
                assert o1.length > keyIndex && o2.length > keyIndex;
                assert o1[keyIndex] instanceof String && o2[keyIndex] instanceof String;
                return ((String) o1[keyIndex]).compareTo((String) o2[keyIndex]);
            });
        }
        batchUpdate(sql, batch);
    }


    protected void batchUpdate(String sql, final List<Object[]> values) {
        if (!values.isEmpty()) {
            checkTransaction();
            log(sql, () -> jdbcTemplate.batchUpdate(sql, new ListBatchPreparedStatementSetter(values)));
        }
    }

    protected <T> T query(String sql, ResultSetExtractor<T> rse, Object... args) throws DataAccessException {
        return log(sql, () -> jdbcTemplate.query(sql, args, rse));
    }

    protected <T> T query(String sql, int maxRows, ResultSetExtractor<T> rse) throws DataAccessException {
        return log(sql, () -> jdbcTemplate.execute(new LimitedRowsStatementCallback<>(sql, maxRows, rse)));
    }

    protected <T> List<T> queryBulk(String query, String field, Collection<String> ids, int maxRows, ResultSetExtractor<List<T>> extractor) {
        if (ids != null && !ids.isEmpty()) {
            if (ids.size() > DEFAULT_BATCH_SIZE) {
                final List<T> result = new ArrayList<>(ids.size());
                final List<List<String>> batches = partition(ids);
                for (List<String> batch : batches) {
                    final Query q = new Query(query).where(field, batch);
                    if (maxRows > 0) {
                        q.setMaxRows(maxRows - result.size());
                    }
                    final List<T> noncached = q.run(extractor);
                    if (noncached != null) {
                        result.addAll(noncached);
                    }
                    if (maxRows > 0 && result.size() >= maxRows) {
                        break;
                    }
                }
                return result;
            } else {
                final Query q = new Query(query).where(field, ids);
                if (maxRows > 0) {
                    q.setMaxRows(maxRows);
                }
                return q.run(extractor);
            }
        } else {
            return Collections.emptyList();
        }
    }

    private static void checkTransaction() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalTransactionStateException("transaction required");
        }
    }

    private <T> T log(String sql, Supplier<T> r) {
        final long start = System.currentTimeMillis();
        final T result = r.get();
        final long finish = System.currentTimeMillis();
        if (finish - start > threshold) {
            logger.info(finish - start + ", " + sql);
        } else {
            logger.debug(sql);
        }
        return result;
    }

    protected class Query {
        private final StringBuilder builder;
        private final List<Object> values = new ArrayList<>();
        private boolean hasFilter;
        private int maxRows;

        public Query(String prefix) {
            builder = new StringBuilder(prefix);
            hasFilter = prefix.contains("where");
        }

        public Query setMaxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public Query set(String field, Object value) {
            if (value != null) {
                values.add(value);
                builder.append(" set ").append(field).append("=?");
            }
            return this;
        }

        public Query where(String field, String value) {
            if (value != null) {
                addFilter();
                values.add(value);
                builder.append(field).append(value.indexOf('%') != -1 ? " like ?" : "=?");
            }
            return this;
        }

        public Query where(String field, Collection<String> value) {
            if (value != null && !value.isEmpty()) {
                addFilter();
                if (value.size() > 1) {
                    values.addAll(value);
                    builder.append(field).append(" in (").append(asParameters(value)).append(")");
                } else {
                    values.add(value.iterator().next());
                    builder.append(field).append(" = ?");
                }
            }
            return this;
        }

        public Query where(String[] fields, String value) {
            if (value != null) {
                addFilter();
                builder.append("(");
                for (int i = 0; i < fields.length; i++) {
                    String field = fields[i];
                    values.add(value);
                    builder.append(field).append(value.indexOf('%') != -1 ? " like ? " : "=? ");
                    if (i < fields.length - 1) {
                        builder.append(" or ");
                    }
                }
                builder.append(")");
            }
            return this;
        }

        public Query and(String field, Serializable value) {
            if (value != null) {
                addFilter();
                final String v = value.toString();
                values.add(v.replace('*', '%'));
                builder.append(field).append(v.indexOf('%') != -1 || v.indexOf('*') != -1 ? " like ?" : "=?");
            }
            return this;
        }

        public Query noVersions() {
            addFilter();
            builder.append("version is null");
            return this;
        }

        public Query versions() {
            addFilter();
            builder.append("version is not null");
            return this;
        }

        public <T> T run(ResultSetExtractor<T> extractor) {
            final String sql = builder.toString();
            if (maxRows != 0) {
                return query(sql, maxRows, extractor);
            } else {
                final Object[] args = values.toArray(new Object[values.size()]);
                return query(sql, extractor, args);
            }
        }

        public List<String> run(String field, Collection<String> ids, ResultSetExtractor<List<String>> extractor) {
            return ids != null && !ids.isEmpty()
                    ? queryBulk(builder.toString(), field, ids, maxRows, extractor)
                    : run(extractor);
        }

        public void update() {
            final String sql = builder.toString();
            final Object[] args = values.toArray(new Object[values.size()]);
            AbstractJdbcDAO.this.update(sql, args);
        }

        private void addFilter() {
            builder.append(hasFilter ? " and " : " where ");
            hasFilter = true;
        }

        private String asParameters(Collection ids) {
            final StringBuilder b = new StringBuilder();
            for (int i = 0; i < ids.size(); i++) {
                b.append('?');
                if (i < ids.size() - 1) {
                    b.append(',');
                }
            }
            return b.toString();
        }
    }

    private static class ListBatchPreparedStatementSetter implements BatchPreparedStatementSetter {
        private final List<Object[]> values;

        public ListBatchPreparedStatementSetter(List<Object[]> values) {
            this.values = values;
        }

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            final Object[] args = values.get(i);
            if (args != null) {
                for (int j = 0; j < args.length; j++) {
                    StatementCreatorUtils.setParameterValue(ps, j + 1, getType(args[j]), getValue(args[j]));
                }
            }
        }

        private Object getValue(Object value) {
            if (TypeConverter.NULL_NUMERIC == value) {
                return null;
            } else if (TypeConverter.NULL_STRING == value) {
                return null;
            } else {
                return value;
            }
        }

        private static int getType(Object value) {
            if (TypeConverter.NULL_NUMERIC == value) {
                return Types.NUMERIC;
            } else if (TypeConverter.NULL_STRING == value) {
                return Types.VARCHAR;
            } else {
                return SqlTypeValue.TYPE_UNKNOWN;
            }
        }

        @Override
        public int getBatchSize() {
            return values.size();
        }
    }

    private static class LimitedRowsStatementCallback<T> implements StatementCallback<T> {
        private final int maxRows;
        private final ResultSetExtractor<T> extractor;
        private final String sql;

        public LimitedRowsStatementCallback(String sql, int maxRows, ResultSetExtractor<T> extractor) {
            this.sql = sql;
            this.maxRows = maxRows;
            this.extractor = extractor;
        }

        @Override
        public T doInStatement(Statement stmt) throws SQLException, DataAccessException {
            ResultSet rs = null;
            try {
                if (maxRows > 0) {
                    stmt.setMaxRows(maxRows);
                }
                rs = stmt.executeQuery(sql);
                return extractor.extractData(rs);
            } finally {
                JdbcUtils.closeResultSet(rs);
            }
        }
    }
}
