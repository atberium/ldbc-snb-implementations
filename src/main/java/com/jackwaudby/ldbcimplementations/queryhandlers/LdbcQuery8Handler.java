package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery8 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ8();
        int limit = operation.getLimit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ")." +
                "in('hasCreator')." +
                "in('replyOf').as('message')." +
                "order().by('creationDate',desc).by('id',asc).limit(" + limit + ")." +
                "out('hasCreator').as('person')." +
                "select('message','person')." +
                "by(valueMap('id','creationDate','content'))." +
                "by(valueMap('id','firstName','lastName'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery8Result> endResult                                   // init result list
                = new ArrayList<>();
        for (final Map<String, String> stringStringHashMap : result) {
            LdbcQuery8Result res                                                // create result object
                    = new LdbcQuery8Result(
                    Long.parseLong(stringStringHashMap.get("personId")),              // personId
                    stringStringHashMap.get("personFirstName"),                       // personFirstName
                    stringStringHashMap.get("personLastName"),                        // personLastName
                    Long.parseLong(stringStringHashMap.get("messageCreationDate")),   // messageCreationDate
                    Long.parseLong(stringStringHashMap.get("messageId")),             // messageId
                    stringStringHashMap.get("messageContent")                         // messageContent
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}
