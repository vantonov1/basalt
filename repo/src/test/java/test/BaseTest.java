package test;

import com.github.vantonov1.basalt.BasaltRepoConfiguration;
import com.github.vantonov1.basalt.repo.impl.RepositoryDAO;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.sql.SQLException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@ContextConfiguration(classes = BasaltRepoConfiguration.class)
@TestPropertySource("classpath:application.properties")
@JdbcTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
public abstract class BaseTest {
    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private RepositoryDAO repositoryDAO;

    @After
    public void after() throws SQLException {
        final Object tx = beginTx(false);
        repositoryDAO.clear();
        commit(tx);
    }

    protected Object beginTx(boolean readOnly) throws SQLException {
        final DefaultTransactionDefinition d = new DefaultTransactionDefinition();
        d.setReadOnly(readOnly);
//        d.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        return transactionManager.getTransaction(d);
    }

    protected void commit(Object tx) throws SQLException {
        if (tx != null) {
            transactionManager.commit((TransactionStatus) tx);
        }
    }

    protected void rollback(Object tx) throws SQLException {
        if (tx != null) {
            transactionManager.rollback((TransactionStatus) tx);
        }
    }
}
