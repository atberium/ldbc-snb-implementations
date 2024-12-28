package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

/**
 * Given a start Person, find that Person’s friends and friends of friends (excluding start Person)
 * who started Working in some Company in a given Country, before a given date (year).
 */
public class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery11 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ11();
        String countryName = operation.getCountryName();
        int workFromYear = operation.getWorkFromYear();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ")." +
                "repeat(both('knows').simplePath()).emit().times(2).dedup().as('person')." +
                "outE('workAt').has('workFrom',lt(" + workFromYear + ")).as('organisationYear')." +
                "inV().as('organisation')." +
                "out('isLocatedIn').has('name','" + countryName + "')." +
                "order()." +
                "by(select('organisationYear').by('workFrom'))." +
                "by(select('person').by('id'))." +
                "by(select('organisation').by('name'),desc)." +
                "select('person','organisation','organisationYear')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('name')).by(valueMap('workFrom'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery11Result> endResult                                   // init result list
                = new ArrayList<>();
        for (final Map<String, String> stringStringHashMap : result) {
            LdbcQuery11Result res                                                // create result object
                    = new LdbcQuery11Result(
                    Long.parseLong(stringStringHashMap.get("personId")),              // personId
                    stringStringHashMap.get("personFirstName"),                       // personFirstName
                    stringStringHashMap.get("personLastName"),                        // personLastName
                    stringStringHashMap.get("organisationName"),
                    Integer.parseInt(stringStringHashMap.get("organisationYearWorkFrom"))
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}
