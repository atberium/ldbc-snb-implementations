package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5Result;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Title: New groups
 * <p>
 * Description: Given a start Person, find the Forums which that Person’s friends and friends of friends
 * (excluding start Person) became Members of after a given date. For each forum find the number of Posts
 * that were created by any of these Persons. For each Forum and consider only those Persons which joined
 * that particular Forum after the given date.
 */
public class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery5 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ5();
        long minDate = operation.getMinDate().getTime();
        long limit = 20;

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup().sideEffect(store('a')).aggregate('friends')." +
                "inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().dedup().as('forums')." +
                "order().by(local(out('containerOf').match(__.as('post').out('hasCreator').where(within('friends')).as('friend')," +
                "__.as('post').in('containerOf').as('forum'),__.as('friend').inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().where(eq('forum')).as('forum')).count()),desc)." +
                "by('id').limit(" + limit + ").local(union(valueMap('title').unfold(),out('containerOf').match(__.as('post').out('hasCreator').where(within('a')).as('friend')," +
                "__.as('post').in('containerOf').as('forum'), __.as('friend').inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().where(eq('forum')).as('forum')).count().fold()).fold())" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<LdbcQuery5Result> endResult                                   // init result list
                = new ArrayList<>();
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);
        if (!results.isEmpty()) {
            for (JSONObject result : results) {
                ArrayList<JSONObject> resultList = gremlinListToArrayList(result);
                LdbcQuery5Result res                                                // create result object
                        = new LdbcQuery5Result(
                        getPropertyValue(gremlinMapToHashMap(resultList.get(0)).get("title")),
                        Integer.parseInt(getPropertyValue(resultList.get(1)))
                );
                endResult.add(res);
            }
        }
        resultReporter.report(0, endResult, operation);
    }

}
