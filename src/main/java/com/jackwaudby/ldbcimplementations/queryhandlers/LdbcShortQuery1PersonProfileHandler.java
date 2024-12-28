package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, gender, creation date and the ID of their city of residence.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonIdSQ1();                                   // get query parameter from operation
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // get JanusGraph client
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Person','id'," + personId + ")." +
                "union(" +
                "valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').unfold()," +
                "out('isLocatedIn').valueMap('id').unfold()" +
                ").fold().toList();" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";

        int TX_ATTEMPTS = 0;                                                                // init. transaction attempts
        int TX_RETRIES = getTxnAttempts();                                                  // get max attempts
        while (TX_ATTEMPTS < TX_RETRIES) {
            log.info("Attempt " + (TX_ATTEMPTS + 1) + ": " +
                    LdbcShortQuery1PersonProfileHandler.class.getSimpleName());
            String response = client.execute(queryString);                                       // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {                         // check if failed
                log.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {
                ArrayList<JSONObject> result = gremlinListToArrayList(results.get(0));               // get result

                try {
                    LdbcShortQuery1PersonProfileResult ldbcShortQuery1PersonProfileResult                // create result object
                            = new LdbcShortQuery1PersonProfileResult(
                            getPropertyValue(gremlinMapToHashMap(result.get(3)).get("firstName")),
                            getPropertyValue(gremlinMapToHashMap(result.get(4)).get("lastName")),
                            Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(6)).get("birthday"))),
                            getPropertyValue(gremlinMapToHashMap(result.get(2)).get("locationIP")),
                            getPropertyValue(gremlinMapToHashMap(result.get(1)).get("browserUsed")),
                            Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(7)).get("id"))),
                            getPropertyValue(gremlinMapToHashMap(result.get(5)).get("gender")),
                            Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(0)).get("creationDate")))
                    );
                    resultReporter.report(0, ldbcShortQuery1PersonProfileResult, operation); // pass to driver
                    break;
                } catch (Exception e) {
                    log.error("Unexpected error", e);
                    TX_ATTEMPTS = TX_ATTEMPTS + 1;
                }
            }
        }
    }
}
