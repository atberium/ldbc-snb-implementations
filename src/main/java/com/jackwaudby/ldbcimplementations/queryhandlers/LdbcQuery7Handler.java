package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ7();
        long limit = operation.getLimit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").in('hasCreator').as('message')." +
                "order().by('creationDate',desc).by('id',asc)" +
                "inE('likes').as('like')." +
                "order().by('creationDate',desc).outV().as('person').dedup().limit(" + limit + ")" +
                "choose(both('knows').has('Person','id'," + personId + "),constant(false),constant(true)).as('isNew')." +
                "select('person','message','isNew','like')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','content','imageFile','creationDate'))." +
                "by(fold())." +
                "by(valueMap('creationDate'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery7Result> endResult                                   // init result list
                = new ArrayList<>();
        for (final Map<String, String> stringStringHashMap : result) {                               // for each result
            String messageContent;                                              // set message content
            if (stringStringHashMap.get("messageContent").isEmpty()) {               // imagefile
                messageContent = stringStringHashMap.get("messageImageFile");
            } else {                                                            // content
                messageContent = stringStringHashMap.get("messageContent");
            }
            final long minutesLatency =
                    (Long.parseLong(stringStringHashMap.get("likeCreationDate")) -
                            Long.parseLong(stringStringHashMap.get("messageCreationDate"))) / 60000;
            final int latency = (int) minutesLatency;
            final boolean isNew = Boolean.parseBoolean(stringStringHashMap.get("isNew"));
            LdbcQuery7Result res                                                // create result object
                    = new LdbcQuery7Result(
                    Long.parseLong(stringStringHashMap.get("personId")),              // personId
                    stringStringHashMap.get("personFirstName"),                       // personFirstName
                    stringStringHashMap.get("personLastName"),                        // personLastName
                    Long.parseLong(stringStringHashMap.get("likeCreationDate")),   // likeCreationDate
                    Long.parseLong(stringStringHashMap.get("messageId")),             // messageId
                    messageContent,
                    latency,
                    isNew
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}
