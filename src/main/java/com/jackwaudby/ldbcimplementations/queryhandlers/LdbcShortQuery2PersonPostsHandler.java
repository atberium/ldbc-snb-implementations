package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPostsResult;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Person, retrieve the last 10 messages created by that user.
 * For each message, return the message ID, content/imageFile and creationDate.
 * Then return the ID of the original post from its conversation and the author of that post, ID, firstName and lastName.
 * If the message is a post then the original post will be the same message.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {


        long limit = operation.getLimit();                                         // message limit
        long personId = operation.getPersonIdSQ2();                                   // start person

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result = " +
                "g.V().has('Person','id'," + personId + ").in('hasCreator')." +
                "order().by('creationDate',decr).by('id',decr).limit(" + limit + ").as('message')." +
                "local(choose(" +
                "hasLabel('Post')," +
                "identity().as('originalPost').out('hasCreator').as('originalAuthor')," +
                "repeat(out('replyOf').simplePath()).until(hasLabel('Post')).as('originalPost').out('hasCreator').as('originalAuthor')))." +
                "select('message','originalPost','originalAuthor')." +
                "by(valueMap('id','imageFile','content','creationDate'))." +
                "by(valueMap('id'))." +
                "by(valueMap('id','firstName','lastName')).toList();" +
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
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery2PersonPostsHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {
                ArrayList<LdbcShortQuery2PersonPostsResult> queryResultList                   // init result list
                        = new ArrayList<>();
                for (JSONObject result : results
                ) {

                    JSONObject message = gremlinMapToHashMap(result).get("message");
                    long messageId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(message).get("id")));
                    long messageCreationDate = Long.parseLong(getPropertyValue(gremlinMapToHashMap(message).get("creationDate")));
                    String messageContent;
                    if (gremlinMapToHashMap(message).containsKey("imageFile") &&
                            !getPropertyValue(gremlinMapToHashMap(message).get("imageFile")).isEmpty()) {
                        messageContent = getPropertyValue(gremlinMapToHashMap(message).get("imageFile"));
                    } else {
                        messageContent = getPropertyValue(gremlinMapToHashMap(message).get("content"));
                    }

                    JSONObject originalPost = gremlinMapToHashMap(result).get("originalPost");
                    long originalPostId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(originalPost).get("id")));

                    JSONObject originalAuthor = gremlinMapToHashMap(result).get("originalAuthor");
                    long originalAuthorId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(originalAuthor).get("id")));
                    String originalAuthorFirstName = getPropertyValue(gremlinMapToHashMap(originalAuthor).get("firstName"));
                    String originalAuthorLastName = getPropertyValue(gremlinMapToHashMap(originalAuthor).get("lastName"));

                    LdbcShortQuery2PersonPostsResult ldbcShortQuery2PersonPostsResult                                 // create result object
                            = new LdbcShortQuery2PersonPostsResult(
                            messageId,
                            messageContent,
                            messageCreationDate,
                            originalPostId,
                            originalAuthorId,
                            originalAuthorFirstName,
                            originalAuthorLastName
                    );
                    queryResultList.add(ldbcShortQuery2PersonPostsResult);
                }
                resultReporter.report(0, queryResultList, operation);              // pass to result reporter
                break;
            }
        }
    }
}
