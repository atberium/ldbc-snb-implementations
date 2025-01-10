package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate1AddPerson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.resultToMap;

@Slf4j
public class LdbcUpdate1AddPersonHandler extends GremlinHandler implements OperationHandler<LdbcUpdate1AddPerson, JanusGraphDb.JanusGraphConnectionState> {
    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long personId = operation.getPersonId();
        final String firstName = operation.getPersonFirstName();
        final String lastName = operation.getPersonLastName();
        final String gender = operation.getGender();
        final long birthday = operation.getBirthday().getTime();
        final long creationDate = operation.getCreationDate().getTime();
        final String locationIP = operation.getLocationIp();
        final String browserUsed = operation.getBrowserUsed();
        final long cityId = operation.getCityId();
        final List<String> languages = operation.getLanguages();
        final String lang = languages.toString().replaceAll(", ", "', '").replaceAll("\\[", "['").replaceAll("]", "']");
        final List<String> email = operation.getEmails();
        final String em = email.toString().replaceAll(", ", "', '").replaceAll("\\[", "['").replaceAll("]", "']");

        final List<Long> tagIds = operation.getTagIds();
        final List<LdbcUpdate1AddPerson.Organization> workAt = operation.getWorkAt();
        final List<Long> companyIds = new ArrayList<>();
        final List<Integer> workFrom = new ArrayList<>();
        for (int i = 0; i < workAt.size(); i++) {
            companyIds.add(i, workAt.get(i).getOrganizationId());
            workFrom.add(i, workAt.get(i).getYear());
        }

        final List<LdbcUpdate1AddPerson.Organization> studyAt = operation.getStudyAt();
        final List<Long> uniIds = new ArrayList<>();
        final List<Integer> classYear = new ArrayList<>();
        for (int i = 0; i < studyAt.size(); i++) {
            uniIds.add(i, studyAt.get(i).getOrganizationId());
            classYear.add(i, studyAt.get(i).getYear());
        }


        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "try {" +
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
                "hm;";

        int attempts = 0;

        while (attempts < TX_RETRIES) {
            log.info("Attempt {}", attempts + 1);
            final List<Result> response = request(client, queryString);
            final Map<String, Object> result = resultToMap(response.get(0));
            if (result.containsKey("query_error")) {
                attempts = attempts + 1;
                log.error("Query Error: {}", result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                attempts = attempts + 1;
                log.error("Gremlin Server Error: {}", result.get("http_error"));
            } else {
                log.info(result.get("query_outcome").toString());
                break;
            }
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);

    }
}
