package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate7AddComment;

import java.util.HashMap;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // comment properties
        long commentId = operation.getCommentId();
        long creationDate = operation.getCreationDate().getTime();
        String locationIp = operation.getLocationIp();
        String browserUsed = operation.getBrowserUsed();
        String content = operation.getContent();
        int length = operation.getLength();

        // outgoing edges
        long authorPersonId = operation.getAuthorPersonId();
        long countryId = operation.getCountryId();
        long replyToPostId = operation.getReplyToPostId();
        long replyToCommentId = operation.getReplyToCommentId();
        List<Long> tagIds = operation.getTagIds();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +
                "try {" +
                "p = g.addV('Comment').property('id'," +
                commentId +
                ").property('creationDate'," +
                creationDate +
                ").property('locationIP','" +
                locationIp +
                "')" +
                ".property('browserUsed','" +
                browserUsed +
//                "').property('content',\\\"" + content + "\\\")" +
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
                "hm;\"" +
                "}";


        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                                  // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);             // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                System.out.println(result.get("query_outcome"));
                break;
            }
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation); // pass result to driver
    }
}
