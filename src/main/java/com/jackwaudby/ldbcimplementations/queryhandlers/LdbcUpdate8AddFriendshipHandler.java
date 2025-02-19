package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate8AddFriendship;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.toMap;

@Slf4j
public class LdbcUpdate8AddFriendshipHandler extends GremlinHandler implements OperationHandler<LdbcUpdate8AddFriendship, JanusGraphDb.JanusGraphConnectionState> {

    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long person1Id = operation.getPerson1Id();
        final long person2Id = operation.getPerson2Id();
        final long creationDate = operation.getCreationDate().getTime();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "try {" +
                "v = g.V().has('Person','id'," +
                person1Id +
                ").next();[];" +
                "g.V().has('Person', 'id'," +
                person2Id +
                ").as('friend').V(v).addE('knows').property('creationDate'," +
                creationDate +
                ").to('friend').next();[];" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;";

        int attempts = 0;

        while (attempts < TX_RETRIES) {
            log.info("Attempt {}", attempts + 1);
            final List<Result> result = request(client, queryString);
            final Map<String, Object> person = toMap(result.get(0));
            if (person.containsKey("query_error")) {
                attempts = attempts + 1;
                log.error("Query Error: {}", person.get("query_error"));
            } else if (person.containsKey("http_error")) {
                attempts = attempts + 1;
                log.error("Gremlin Server Error: {}", person.get("http_error"));
            } else {
                log.info(person.get("query_outcome").toString());
                break;
            }
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
}
