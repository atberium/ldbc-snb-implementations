package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate1AddPerson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, JanusGraphDb.JanusGraphConnectionState> {


    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.getPersonId();
        String firstName = operation.getPersonFirstName();
        String lastName = operation.getPersonLastName();
        String gender = operation.getGender();
        long birthday = operation.getBirthday().getTime();
        long creationDate = operation.getCreationDate().getTime();
        String locationIP = operation.getLocationIp();
        String browserUsed = operation.getBrowserUsed();
        long cityId = operation.getCityId();
        List<String> languages = operation.getLanguages();
        String lang = languages.toString().replaceAll(", ", "', '").replaceAll("\\[", "['").replaceAll("]", "']");
        List<String> email = operation.getEmails();
        String em = email.toString().replaceAll(", ", "', '").replaceAll("\\[", "['").replaceAll("]", "']");

        List<Long> tagIds = operation.getTagIds();
        List<LdbcUpdate1AddPerson.Organization> workAt = operation.getWorkAt();
        List<Long> companyIds = new ArrayList<>();
        List<Integer> workFrom = new ArrayList<>();
        for (int i = 0; i < workAt.size(); i++) {
            companyIds.add(i, workAt.get(i).getOrganizationId());
            workFrom.add(i, workAt.get(i).getYear());
        }

        List<LdbcUpdate1AddPerson.Organization> studyAt = operation.getStudyAt();
        List<Long> uniIds = new ArrayList<>();
        List<Integer> classYear = new ArrayList<>();
        for (int i = 0; i < studyAt.size(); i++) {
            uniIds.add(i, studyAt.get(i).getOrganizationId());
            classYear.add(i, studyAt.get(i).getYear());
        }

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"try {" +
                "p = g.addV('Person')" +
                ".property('id'," + personId + ")" +
                ".property('firstName','" + firstName + "')" +
                ".property('lastName','" + lastName + "')" +
                ".property('gender','" + gender + "')" +
                ".property('birthday','" + birthday + "')" +
                ".property('creationDate','" + creationDate + "')" +
                ".property('locationIP','" + locationIP + "')" +
                ".property('browserUsed','" + browserUsed + "').next();[];" +
                "g.V().has('Place', 'id'," + cityId + ").as('city').V(p).addE('isLocatedIn').to('city').next();[];" +
                "languages=" + lang + ";[];" +
                "for (item in languages) { " +
                " g.V(p).property(list, 'language', item).next();[];" +
                "}; " +
                "email=" + em + ";[];" +
                "for (item in email) { " +
                "g.V(p).property(list, 'email', item).next();[];" +
                "}; " +
                "tagid=" +
                tagIds.toString() +
                ";[];" +
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasInterest').to('tag').next();[];" +
                "};" +
                "companyId=" +
                companyIds +
                ";[];" +
                "workFrom=" +
                workFrom +
                ";[];" +
                "for (i = 0; i < companyId.size();i++){" +
                "g.V().has('Organisation', 'id', companyId[i]).as('comp').V(p).addE('workAt').property('workFrom',workFrom[i]).to('comp').next();[];" +
                "};" +
                "uniId=" +
                uniIds +
                ";[];" +
                "classYear=" +
                classYear +
                ";[];" +
                "for (i = 0; i < uniId.size();i++){" +
                "g.V().has('Organisation', 'id', uniId[i]).as('uni').V(p).addE('studyAt').property('classYear',classYear[i]).to('uni').next();[];" +
                "};" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;

        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                                // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                System.out.println(result.get("query_outcome"));
                break;
            }
        }


        // pass result to driver
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);

    }
}
