package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.getPropertyValue;
import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.gremlinResponseToResultArrayList;

/**
 * Title: Single shortest path
 * <p>
 * Description: Given two Persons, find the shortest path between these two Persons in the subgraph induced
 * by the Knows relationships.
 */
public class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery13 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long person1Id = operation.getPerson1IdQ13StartNode();
        long person2Id = operation.getPerson2IdQ13EndNode();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + person1Id + ")." +
                "choose(" +
                "repeat(both('knows').dedup()).until(has('Person','id'," + person2Id + ")).limit(1).path().count(local).is(gt(0))," +
                "repeat(store('x').both('knows').where(without('x')).aggregate('x')).until(has('Person','id'," + person2Id + ")).limit(1).path().count(local)," +
                "constant(-1)).fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);
        int shortestPathLength = Integer.parseInt(getPropertyValue(resultList.get(0)));
        if (shortestPathLength != -1) {
            shortestPathLength = shortestPathLength - 1;
        }
        // for each result
        LdbcQuery13Result endResult                                                // create result object
                = new LdbcQuery13Result(
                shortestPathLength
        );
        resultReporter.report(0, endResult, operation);


    }
}
