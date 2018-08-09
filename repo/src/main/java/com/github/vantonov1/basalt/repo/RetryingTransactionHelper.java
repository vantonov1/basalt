package com.github.vantonov1.basalt.repo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.Random;

/**
 * Utility class to support transactions  with optimistic locking. Execution inside {@link RetryingTransactionHelper#doInTransaction} will be automatically retried in case of {@link TransientDataAccessException} up to 100 times
 */
@SuppressWarnings("unused")
public class RetryingTransactionHelper {
    private static final int MAX_RETRIES = 100;
    private static final int MIN_RETRY_WAIT = 200;
    private static final int MAX_RETRY_WAIT = 1000;
    private static final int RETRY_WAIT_INCREMENT = 100;

    private final Log logger = LogFactory.getLog(getClass());

    @FunctionalInterface
    public interface TransactionCallback<T> {
        T execute() throws Exception;
    }

    private final PlatformTransactionManager transactionManager;

    public RetryingTransactionHelper(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Execute arbitrary code inside transaction. Transaction is automatically rolled back and repeated in case of transient exceptions, probably related to optimistic locking
     * @return value returned by callback
     */
    public <T> T doInTransaction(boolean readOnly, TransactionCallback<T> cb) {
        final Random random = new Random(System.currentTimeMillis());
        RuntimeException lastException = null;
        for (int count = 0; count < MAX_RETRIES; count++) {
            Object tx = null;
            try {
                final DefaultTransactionDefinition d = new DefaultTransactionDefinition();
                d.setReadOnly(readOnly);
                tx = transactionManager.getTransaction(d);
                final T result = cb.execute();
                transactionManager.commit((TransactionStatus) tx);
                return result;
            } catch (Throwable t) {
                if (tx != null) {
                    try {
                        transactionManager.rollback((TransactionStatus) tx);
                    } catch (Exception e) {
                        logger.error("while rolling back tx", e);
                    }
                }
                lastException = (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
                if (isRetryNeeded(t)) {
                    delay(random, count);
                } else {
                    throw lastException;
                }
            }
        }
        throw lastException;
    }

    protected void delay(Random random, int count) {
        int interval = MIN_RETRY_WAIT + random.nextInt(count > 0 ? count * RETRY_WAIT_INCREMENT : MIN_RETRY_WAIT);
        if (interval >= MAX_RETRY_WAIT) {
            interval = MAX_RETRY_WAIT - random.nextInt(MIN_RETRY_WAIT);
        }
        try {
            Thread.sleep(interval);
        } catch (InterruptedException ignored) {
        }
    }

    protected boolean isRetryNeeded(Throwable cause) {
        while (cause != null) {
            if (TransientDataAccessException.class.isAssignableFrom(cause.getClass())) {
                return true;
            }
            final Throwable next = cause.getCause();
            cause = (cause == next) ? null : next;
        }
        return false;
    }
}
