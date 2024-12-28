package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONObject;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result;

import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Title: Friends with certain name
 * <p>
 * Description: Given a start Person, find Persons with a given first name that the start Person is connected to
 * (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including the distance (1..3),
 * summaries of the Persons workplaces and places of study; sorted by distanceFromPerson (asc),
 * personLastName (asc), personId (asc).
 */
public class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery1 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        long personId = operation.getPersonIdQ1();                       // person ID
        String personFirstName = operation.getFirstName();             // person first name
        int limit = operation.getLimit();                              // result limit

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.withSack(0).V().has('Person','id'," + personId + ")." +
                "repeat(both('knows').simplePath().sack(sum).by(constant(1))).emit().times(3)." +
                "dedup().has('Person','firstName','" + personFirstName + "')." +
                "order().by(sack(),asc).by('lastName',asc).by('id',asc).limit(" + limit + ")" +
                "local(union(" +
                "valueMap('lastName','id','email','birthday','creationDate','gender','browserUsed','locationIP','language'), " +
                "out('isLocatedIn').valueMap('name')," +
                "sack().fold(), " +
                "outE('workAt').as('workFrom').inV().as('company').out('isLocatedIn').as('country')." +
                "select('company','workFrom','country').by(valueMap('name')).by(valueMap()).fold()," +
                "outE('studyAt').as('studyFrom').inV().as('university').out('isLocatedIn').as('country')." +
                "select('university','studyFrom','country').by(valueMap('name')).by(valueMap()).fold()).fold())" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList =
                gremlinResponseToResultArrayList(response);                     // convert response into result list
        ArrayList<LdbcQuery1Result> endResult                                   // init end result list
                = new ArrayList<>();
        for (JSONObject result : resultList) {                                  // for each result

            ArrayList<JSONObject> resultBreakdown = gremlinListToArrayList(result);             // result contains sub results
            HashMap<String, JSONObject> person = gremlinMapToHashMap(resultBreakdown.get(0));    // person result
            HashMap<String, JSONObject> city = gremlinMapToHashMap(resultBreakdown.get(1));      // city result
            int distanceFrom = Integer.parseInt(getPropertyValue(resultBreakdown.get(2)));      // distance result
            String emails = getPropertyValue(person.get("email"));                              // email result
            List<String> emailList =
                    Arrays.asList(
                            emails.replaceAll("[\\[\\]\\s+]", "").split(","));
            if (emailList.size() == 1 && emailList.get(0).isEmpty()) {
                emailList = new ArrayList<>();
            }
            String speaks = getPropertyValue(person.get("language"));                             // speaks result
            List<String> speaksList = Arrays.asList(
                    speaks.replaceAll("[\\[\\]\\s+]", "").split(","));

            final List<JSONObject> universities = gremlinListToArrayList(resultBreakdown.get(4));
            final List<LdbcQuery1Result.Organization> universitiesResult = new ArrayList<>();

            if (!universities.isEmpty()) {
                for (JSONObject u : universities) { // foreach uni
                    final Map<String, JSONObject> universityMap = gremlinMapToHashMap(u);
                    final String countryName =
                            getPropertyValue(gremlinMapToHashMap(universityMap.get("country")).get("name"));
                    final String universityName =
                            getPropertyValue(gremlinMapToHashMap(universityMap.get("university")).get("name"));
                    final int classYear =
                            Integer.parseInt(gremlinMapToHashMap(universityMap.get("studyFrom")).get("classYear").get("@value").toString());

                    universitiesResult.add(new LdbcQuery1Result.Organization(
                            universityName,
                            classYear,
                            countryName
                    ));
                }
            }

            final List<JSONObject> companies = gremlinListToArrayList(resultBreakdown.get(3));
            final List<LdbcQuery1Result.Organization> companiesResult = new ArrayList<>();

            if (!companies.isEmpty()) {
                for (JSONObject u : companies) {
                    final Map<String, JSONObject> companyMap = gremlinMapToHashMap(u);

                    final String countryName = getPropertyValue(gremlinMapToHashMap(companyMap.get("country")).get("name"));
                    final String companyName = getPropertyValue(gremlinMapToHashMap(companyMap.get("company")).get("name"));
                    int workFrom = Integer.parseInt(
                            gremlinMapToHashMap(companyMap.get("workFrom")).get("workFrom").get("@value").toString());

                    companiesResult.add(new LdbcQuery1Result.Organization(
                            companyName,
                            workFrom,
                            countryName
                    ));
                }
            }

            LdbcQuery1Result res                                                    // create result object
                    = new LdbcQuery1Result(
                    Long.parseLong(getPropertyValue(person.get("id"))),             // personId
                    getPropertyValue(person.get("lastName")),                       // personLastName
                    distanceFrom,                                                   // distanceFrom
                    Long.parseLong(getPropertyValue(person.get("birthday"))),       // birthday
                    Long.parseLong(getPropertyValue(person.get("creationDate"))),   // creationDate
                    getPropertyValue(person.get("gender")),                         // gender
                    getPropertyValue(person.get("browserUsed")),                    // browser used
                    getPropertyValue(person.get("locationIP")),                     // location ip
                    emailList,                                                      // email list
                    speaksList,                                                     // speaks list
                    getPropertyValue(city.get("name")),                             // city name
                    universitiesResult,                                             // universities result
                    companiesResult                                                 // companies result
            );
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);
    }
}
