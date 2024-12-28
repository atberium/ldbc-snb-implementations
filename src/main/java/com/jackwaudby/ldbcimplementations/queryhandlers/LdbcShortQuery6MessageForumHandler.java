package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForumResult;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message ID, retrieve the Forum that contains it (ID and title) and
 * retrieve the ID, firstName and lastName of the Person that moderates the Forum.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.getMessageForumId();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client
        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result=g.V().has('Post','id'," + messageId + ").fold()." +
                "coalesce(unfold(),V().has('Comment','id'," + messageId + ")." +
                "repeat(out('replyOf').simplePath()).until(hasLabel('Post')))." +
                "in('containerOf').as('forum').out('hasModerator').as('moderator')." +
                "select('forum','moderator')." +
                "by(valueMap('id','title'))." +
                "by(valueMap('id','firstName','lastName')).toList();[];" +
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
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery6MessageForumHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {

                JSONObject forum = gremlinMapToHashMap(results.get(0)).get("forum");
                long forumId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(forum).get("id")));
                String forumTitle = getPropertyValue(gremlinMapToHashMap(forum).get("title"));

                JSONObject moderator = gremlinMapToHashMap(results.get(0)).get("moderator");
                long moderatorId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(moderator).get("id")));
                String moderatorFirstName = getPropertyValue(gremlinMapToHashMap(moderator).get("firstName"));
                String moderatorLastName = getPropertyValue(gremlinMapToHashMap(moderator).get("lastName"));

                LdbcShortQuery6MessageForumResult queryResult = new LdbcShortQuery6MessageForumResult(
                        forumId,
                        forumTitle,
                        moderatorId,
                        moderatorFirstName,
                        moderatorLastName
                );

                resultReporter.report(0, queryResult, operation);
                break;
            }
        }


    }
}
