package com.github.vantonov1.basalt.acl.impl;

import com.github.vantonov1.basalt.repo.AbstractJdbcDAO;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
class AclDAO extends AbstractJdbcDAO {
    private static final ResultSetExtractor<List<ACE>> GET_ACES = rs -> {
        List<ACE> result = null;
        int index = 0;
        while (rs.next()) {
            setFetchSize(rs, index++);
            if (result == null) {
                result = new ArrayList<>();
            }
            final String node_id = rs.getString("node_id");
            final String authority_id = rs.getString("authority_id");
            final int mask = rs.getShort("mask");
            result.add(new ACE(node_id, authority_id != null ? authority_id.trim() : null, mask));
        }
        return result;
    };

    public AclDAO(DataSource dataSource) {
        super(dataSource);
    }

    public List<ACE> getAces(String id) {
        return query("select * from bst_aces where node_id = ?", GET_ACES, id);
    }

    public List<ACE> getAces(Collection<String> ids) {
        return queryBulk("select * from bst_aces", "node_id", ids, -1, GET_ACES);
    }

    public void createAces(String id, Collection<ACE> aces) {
        if(aces.size() == 1) {
            final ACE ace = aces.iterator().next();
            update("insert into bst_aces (node_id, authority_id, mask) values (?, ?, ?)", id, ace.authorityId, ace.mask);
        } else {
            final List<Object[]> batch = aces.stream().map(ace -> new Object[]{id, ace.authorityId, ace.mask}).collect(Collectors.toList());
            batchUpdate("insert into bst_aces (node_id, authority_id, mask) values (?, ?, ?)", batch);
        }
    }

    public void removeAces(String id, String authority) {
        new Query("delete from bst_aces").where("node_id", id).and("authority_id", authority).update();
    }

    public void convertAcesTo(String id, int mask) {
        update("update bst_aces set mask = ? where node_id = ?", mask, id);
    }

}
