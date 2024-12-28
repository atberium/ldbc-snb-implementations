package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery2 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonIdQ2();
        long date = operation.getMaxDate().getTime();
        int limit = operation.getLimit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").both('knows').as('person')." +
                "in('hasCreator').has('creationDate',lte(new Date(" + date + ")))." +
                "order().by('creationDate',desc).by('id',asc).limit(" + limit + ").as('message')." +
                "select('person','message')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','imageFile','content','creationDate'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery2Result> endResult                                   // init result list
                = new ArrayList<>();
        for (final Map<String, String> stringStringHashMap : result) {
            String messageContent;                                              // set message content
            if (stringStringHashMap.get("messageContent").isEmpty()) {               // imagefile
                messageContent = stringStringHashMap.get("messageImageFile");
            } else {                                                            // content
                messageContent = stringStringHashMap.get("messageContent");
            }
            LdbcQuery2Result res                                                // create result object
                    = new LdbcQuery2Result(
                    Long.parseLong(stringStringHashMap.get("personId")),              // personId
                    stringStringHashMap.get("personFirstName"),                       // personFirstName
                    stringStringHashMap.get("personLastName"),                        // personLastName
                    Long.parseLong(stringStringHashMap.get("messageId")),             // messageId
                    messageContent, // messageContent
                    Long.parseLong(stringStringHashMap.get("messageCreationDate"))    // messageCreationDate
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}
