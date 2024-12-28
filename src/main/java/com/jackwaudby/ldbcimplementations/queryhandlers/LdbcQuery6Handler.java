package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONArray;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import java.util.ArrayList;

public class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery6 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonIdQ6();
        String tagName = operation.getTagName();
        int limit = operation.getLimit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "in('hasCreator').hasLabel('Post')." +
                "where(out('hasTag').has('Tag','name','" + tagName + "'))." +
                "out('hasTag').has('Tag','name',neq('" + tagName + "'))." +
                "groupCount().by('name').order(local).by(values,desc).by(keys,asc).unfold().limit(" + limit + ").fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        JSONObject responseJson = new JSONObject(response);                         // convert to JSON
        JSONArray results = responseJson.getJSONObject("result").                        // get results
                getJSONObject("data").
                getJSONArray("@value").
                getJSONObject(0).getJSONArray("@value");

        ArrayList<LdbcQuery6Result> endResult                                   // init result list
                = new ArrayList<>();

        for (int i = 0; i < results.length(); i++) {
            String key = results.getJSONObject(i).getJSONArray("@value").getString(0);
            int value = results.getJSONObject(i).getJSONArray("@value").getJSONObject(1).getInt("@value");
            LdbcQuery6Result res = new LdbcQuery6Result(
                    key,
                    value
            );
            endResult.add(res);
        }

        resultReporter.report(0, endResult, operation);
    }
}
