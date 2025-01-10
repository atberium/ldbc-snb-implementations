package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate2AddPostLike;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.resultToMap;

@Slf4j
public class LdbcUpdate2AddPostLikeHandler extends GremlinHandler implements OperationHandler<LdbcUpdate2AddPostLike, JanusGraphDb.JanusGraphConnectionState> {

    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {


        final long personId = operation.getPersonId();
        final long postId = operation.getPostId();
        final long creationDate = operation.getCreationDate().getTime();


        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();


        final String queryString = "try {" +
                "v = g.V().has('Person','id'," +
                personId +
                ").next();[];" +
                "g.V().has('Post', 'id'," +
                postId +
                ").as('post').V(v).addE('likes').property('creationDate'," +
                creationDate +
                ").to('post').next();[];" +
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
            List<Result> response = request(client, queryString);
            final Map<String, Object> result = resultToMap(response.get(0));
            if (result.containsKey("query_error")) {
                attempts = attempts + 1;
                log.error("Query Error: {}", result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                attempts = attempts + 1;
                log.error("Gremlin Server Error: {}", result.get("http_error"));
            } else {
                log.info(result.get("query_outcome").toString());
                break;
            }
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
}
