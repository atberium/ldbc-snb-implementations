package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3Result;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Title: Friends and friends of friends that have been to countries X and Y
 * <p>
 * Description: Given a start Person, find Persons that are their friends and friends of friends
 * that have made Posts/Comments in both of the given Countries, X and Y, within a given period.
 * Only Persons that are foreign to Countries X and Y are considered, that is Persons whose
 * Location is not Country X or Country Y.
 * <p>
 * Return: PersonID, firstName, lastName, xCount, yCount, count. Sort by xCount (desc) and personID (asc)
 */
public class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery3 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonIdQ3();
        String countryX = operation.getCountryXName();
        String countryY = operation.getCountryYName();
        long startDate = operation.getStartDate().getTime();
        long duration = operation.getDurationDays();
        long endDate = startDate + (duration * 86400000L);
        long limit = 20;

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "where(and(out('isLocatedIn').out('isPartOf').has('name',without('" + countryX + "','" + countryY + "'))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryX + "').count().is(gt(0)))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryY + "').count().is(gt(0)))))." +
                "order().by(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryX + "').count(),desc).by('id',asc).limit(" + limit + ")." +
                "local(union(identity().valueMap('id','firstName','lastName').unfold(),__.in('hasCreator')." +
                "has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name',within('" + countryX + "','" + countryY + "')).group().by('name').by(count().fold()).unfold()).fold())" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<LdbcQuery3Result> endResult                                   // init result list
                = new ArrayList<>();
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);
        if (!results.isEmpty()) {
            for (JSONObject result : results) {
                ArrayList<JSONObject> resultList = gremlinListToArrayList(result);
                long countryXCount;
                long countryYCount;
                try {
                    countryXCount = Long.parseLong(getPropertyValue(gremlinMapToHashMap(resultList.get(3)).get(countryX)));
                    countryYCount = Long.parseLong(getPropertyValue(gremlinMapToHashMap(resultList.get(4)).get(countryY)));
                } catch (NullPointerException e) {
                    countryYCount = Long.parseLong(getPropertyValue(gremlinMapToHashMap(resultList.get(3)).get(countryY)));
                    countryXCount = Long.parseLong(getPropertyValue(gremlinMapToHashMap(resultList.get(4)).get(countryX)));
                }
                LdbcQuery3Result res                                                // create result object
                        = new LdbcQuery3Result(
                        Long.parseLong(getPropertyValue(gremlinMapToHashMap(resultList.get(0)).get("id"))),
                        getPropertyValue(gremlinMapToHashMap(resultList.get(1)).get("firstName")),
                        getPropertyValue(gremlinMapToHashMap(resultList.get(2)).get("lastName")),
                        countryXCount,
                        countryYCount,
                        countryXCount + countryYCount);
                endResult.add(res);
            }
        }
        resultReporter.report(0, endResult, operation);
    }

}
