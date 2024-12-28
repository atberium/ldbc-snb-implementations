package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageReplies;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageRepliesResult;

import java.util.ArrayList;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message, retrieve the (1-hop) Comments that reply to it.
 * In addition, return a boolean flag knows indicating if the author of the reply knows the author of the original message.
 * If author is same as original author, return false for knows flag.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.getMessageRepliesId();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "graph.tx().rollback();[];" +
                "try{" +
                "originalMessage=g.V().has('Post','id'," + messageId + ").fold().coalesce(unfold(),V().has('Comment','id'," + messageId + ")).next();[];" +
                "originalAuthor=g.V(originalMessage).out('hasCreator').next();[];" +
                "result=g.V(originalMessage).as('originalMessage').in('replyOf').as('comment')." +
                "order().by(select('comment').by('creationDate'),desc).by(select('comment').by('id'),asc)." +
                "out('hasCreator').as('replyAuthor')." +
                "choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows')." +
                "select('comment','replyAuthor','knows')." +
                "by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by(fold())." +
                "map{it -> [" +
                "commentId:it.get().get('comment').get('id')," +
                "commentContent:it.get().get('comment').get('content')," +
                "commentCreationDate:it.get().get('comment').get('creationDate')," +
                "replyAuthorId:it.get().get('replyAuthor').get('id')," +
                "replyAuthorFirstName:it.get().get('replyAuthor').get('firstName')," +
                "replyAuthorLastName:it.get().get('replyAuthor').get('lastName')," +
                "replyAuthorKnowsOriginalMessageAuthor:it.get().get('knows')" +
                "]};[];" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";

        int TX_ATTEMPTS = 0;                                                                 // init. transaction attempts
        int TX_RETRIES = getTxnAttempts();

        while (TX_ATTEMPTS < TX_RETRIES) {
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery7MessageRepliesHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {
                log.info("B");

                final List<LdbcShortQuery7MessageRepliesResult> queryResults = new ArrayList<>();
                for (JSONObject result : results) {
                    long commentId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("commentId")));
                    String commentContent = getPropertyValue(gremlinMapToHashMap(result).get("commentContent"));
                    long commentCreationDate = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("commentCreationDate")));
                    long replyAuthorId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorId")));
                    String replyAuthorFirstName = getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorFirstName"));
                    String replyAuthorLastName = getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorLastName"));
                    boolean replyAuthorKnowsOriginalMessageAuthor = gremlinMapToHashMap(result).get("replyAuthorKnowsOriginalMessageAuthor").getJSONArray("@value").getBoolean(0);
                    LdbcShortQuery7MessageRepliesResult queryResult                                    // create result object
                            = new LdbcShortQuery7MessageRepliesResult(
                            commentId,
                            commentContent,
                            commentCreationDate,
                            replyAuthorId,
                            replyAuthorFirstName,
                            replyAuthorLastName,
                            replyAuthorKnowsOriginalMessageAuthor
                    );
                    queryResults.add(queryResult);
                }

                log.info("Number of results: {}", queryResults.size());

                resultReporter.report(0, queryResults, operation);

                break;
            }
        }
    }
}
