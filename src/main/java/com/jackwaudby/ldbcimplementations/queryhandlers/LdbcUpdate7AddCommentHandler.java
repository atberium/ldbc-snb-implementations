package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate7AddComment;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.resultToMap;

@Slf4j
public class LdbcUpdate7AddCommentHandler extends GremlinHandler implements OperationHandler<LdbcUpdate7AddComment, JanusGraphDb.JanusGraphConnectionState> {

    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long commentId = operation.getCommentId();
        final long creationDate = operation.getCreationDate().getTime();
        final String locationIp = operation.getLocationIp();
        final String browserUsed = operation.getBrowserUsed();
        final String content = operation.getContent();
        final int length = operation.getLength();
        final long authorPersonId = operation.getAuthorPersonId();
        final long countryId = operation.getCountryId();
        final long replyToPostId = operation.getReplyToPostId();
        final long replyToCommentId = operation.getReplyToCommentId();
        final List<Long> tagIds = operation.getTagIds();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "try {" +
                "p = g.addV('Comment').property('id'," +
                commentId +
                ").property('creationDate'," +
                creationDate +
                ").property('locationIP','" +
                locationIp +
                "')" +
                ".property('browserUsed','" +
                browserUsed +
                "').property('content','''" + content + "''')" +
                ".property('length'," +
                length +
                ").next();[];" +
                "g.V().has('Person', 'id'," +
                authorPersonId +
                ").as('creator').V(p).addE('hasCreator').to('creator').next();[];" +
                "g.V().has('Place', 'id'," +
                countryId +
                ").as('location').V(p).addE('isLocatedIn').to('location').next();[];" +
                "tagid=" +
                tagIds.toString() +
                ";[];" +
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
                "if (" + replyToPostId + "==-1){" +
                "g.V().has('Comment', 'id'," + replyToCommentId + ").as('comment').V(p).addE('replyOf').to('comment').next();[];" +
                "} else {" +
                "g.V().has('Post', 'id'," + replyToPostId + ").as('post').V(p).addE('replyOf').to('post').next();[];" +
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
