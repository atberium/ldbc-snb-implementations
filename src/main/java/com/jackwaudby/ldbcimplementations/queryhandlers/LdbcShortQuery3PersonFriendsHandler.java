package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriendsResult;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a start Person, retrieve all of their friend's ID, firstName, lastName, and the date at which they became friends.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonIdSQ3();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Person','id'," + personId + ").bothE('knows').as('friendshipCreationDate').otherV().as('friend')." +
                "order().by(select('friendshipCreationDate').by('creationDate'),desc)." +
                "by('id',asc).select('friendshipCreationDate','friend')." +
                "by(values('creationDate').fold()).by(valueMap('id','firstName','lastName')).toList();" +
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
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery3PersonFriendsHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {
                ArrayList<LdbcShortQuery3PersonFriendsResult> queryResultList                   // init result list
                        = new ArrayList<>();
                for (JSONObject result : results
                ) {
                    long friendshipCreationDate = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("friendshipCreationDate")));

                    JSONObject friend = gremlinMapToHashMap(result).get("friend");
                    long originalPostId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(friend).get("id")));
                    String originalAuthorFirstName = getPropertyValue(gremlinMapToHashMap(friend).get("firstName"));
                    String originalAuthorLastName = getPropertyValue(gremlinMapToHashMap(friend).get("lastName"));

                    LdbcShortQuery3PersonFriendsResult ldbcShortQuery3PersonFriendsResult =
                            new LdbcShortQuery3PersonFriendsResult(
                                    originalPostId,
                                    originalAuthorFirstName,
                                    originalAuthorLastName,
                                    friendshipCreationDate);
                    queryResultList.add(ldbcShortQuery3PersonFriendsResult);
                }
                resultReporter.report(0, queryResultList, operation);              // pass to result reporter
                break;
            }
        }
    }
}
