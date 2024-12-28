package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreatorResult;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message, retrieve its author and their ID, firstName and lastName.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.getMessageIdCreator();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result=g.V().has('Comment','id'," + messageId + ").fold()" +
                ".coalesce(unfold(),V().has('Post','id'," + messageId + "))" +
                ".out('hasCreator').valueMap('id','firstName','lastName').toList();[];" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = getTxnAttempts();

        while (TX_ATTEMPTS < TX_RETRIES) {
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery5MessageCreatorHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {

                long personId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("id")));
                String firstName = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("firstName"));
                String lastName = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("lastName"));

                LdbcShortQuery5MessageCreatorResult queryResult = new LdbcShortQuery5MessageCreatorResult(                 // create result object
                        personId,
                        firstName,
                        lastName
                );
                resultReporter.report(0, queryResult, operation);

                break;
            }
        }


    }
}
