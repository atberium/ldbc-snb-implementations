package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONArray;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import java.util.ArrayList;

public class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery4 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ4();
        long startDate = operation.getStartDate().getTime();
        long duration = (operation.getDurationDays() * 24L * 60L * 60L * 1000L);
        long endDate = startDate + duration;
        long limit = operation.getLimit();


        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").both('knows').in('hasCreator').sideEffect(has('Post','creationDate',lt(new Date(" + startDate + "))).out('hasTag').aggregate('oldTags')).has('Post','creationDate',between(new Date(" + startDate + "),new Date(" + endDate + "))).out('hasTag').where(without('oldTags')).order().by('name').group().by('name').by(count()).order(local).by(values,desc).by(keys,asc).unfold().limit(" + limit + ").fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        JSONObject responseJson = new JSONObject(response);                         // convert to JSON
        JSONArray results = responseJson.getJSONObject("result").                        // get results
                getJSONObject("data").
                getJSONArray("@value").
                getJSONObject(0).getJSONArray("@value");

        ArrayList<LdbcQuery4Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            String key = results.getJSONObject(i).getJSONArray("@value").getString(0);
            int value = results.getJSONObject(i).getJSONArray("@value").getJSONObject(1).getInt("@value");
            LdbcQuery4Result res = new LdbcQuery4Result(key, value);
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);

    }
}
