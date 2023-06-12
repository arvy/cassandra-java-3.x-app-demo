package cassandra.java.app.demo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class ArvyRetryPolicy implements RetryPolicy {
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if( nbRetry < 3 ){
            RetryDecision.tryNextHost(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if( nbRetry < 3 ){
            RetryDecision.tryNextHost(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        if( nbRetry < 3 ){
            RetryDecision.tryNextHost(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        if( nbRetry < 3 ){
            RetryDecision.tryNextHost(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public void init(Cluster cluster) {

    }

    @Override
    public void close() {

    }
}
