package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery9 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ9();
        long date = operation.getMaxDate().getTime();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ")." +
                "repeat(both('knows').simplePath()).emit().times(2).hasLabel('Person').dedup().as('person')." +
                "in('hasCreator').has('creationDate',lt(new Date(" + date + ")))." +
                "order().by('creationDate',desc).by('id',asc).limit(20).as('message')." +
                "select('person','message').by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','creationDate','content','imageFile'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery9Result> endResult                                   // init result list
                = new ArrayList<>();
        for (final Map<String, String> stringStringHashMap : result) {
            String messageContent;                                              // set message content
            if (stringStringHashMap.get("messageContent").isEmpty()) {               // imagefile
                messageContent = stringStringHashMap.get("messageImageFile");
            } else {                                                            // content
                messageContent = stringStringHashMap.get("messageContent");
            }
            LdbcQuery9Result res                                                // create result object
                    = new LdbcQuery9Result(
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
