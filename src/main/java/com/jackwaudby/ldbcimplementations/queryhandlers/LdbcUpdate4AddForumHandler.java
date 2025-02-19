package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate4AddForum;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.resultToMap;

@Slf4j
public class LdbcUpdate4AddForumHandler extends GremlinHandler implements OperationHandler<LdbcUpdate4AddForum, JanusGraphDb.JanusGraphConnectionState> {

    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long forumId = operation.getForumId();
        final String forumTitle = operation.getForumTitle();
        final long forumCreationDate = operation.getCreationDate().getTime();
        final long moderatorId = operation.getModeratorPersonId();
        final List<Long> tagIds = operation.getTagIds();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "try {" +
                "p = g.addV('Forum')" +
                ".property('id'," + forumId + ")" +
                ".property('title','" + forumTitle + "')" +
                ".property('creationDate','" + forumCreationDate + "').next();[];" +
                "g.V().has('Person', 'id'," + moderatorId + ").as('person').V(p).addE('hasModerator').to('person').next();[];" +
                "tagid=" + tagIds.toString() + ";[];" +
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
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
            final List<Result> response = request(client, queryString);
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
