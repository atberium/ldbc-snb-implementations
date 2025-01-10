package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery1Result;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

/**
 * Title: Friends with certain name
 * <p>
 * Description: Given a start Person, find Persons with a given first name that the start Person is connected to
 * (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including the distance (1..3),
 * summaries of the Persons workplaces and places of study; sorted by distanceFromPerson (asc),
 * personLastName (asc), personId (asc).
 */
public class LdbcQuery1Handler extends GremlinHandler implements OperationHandler<LdbcQuery1, JanusGraphDb.JanusGraphConnectionState> {

    @SuppressWarnings("unchecked")
    private static List<LdbcQuery1Result.Organization> getUniversities(@NonNull Object universitiesObject) {
        return ((List<Map<String, Object>>) universitiesObject).stream()
                .map(u -> new LdbcQuery1Result.Organization(
                        parseUniversityName(u),
                        parseClassYear(u),
                        parseCountryName(u)
                ))
                .collect(toList());
    }

    @SuppressWarnings("unchecked")
    private static List<LdbcQuery1Result.Organization> getCompanies(@NonNull Object companiesObject) {
        return ((List<Map<String, Object>>) companiesObject).stream()
                .map(c -> new LdbcQuery1Result.Organization(
                        parseCountryName(c),
                        parseWorkFrom(c),
                        parseCountryName(c)
                ))
                .collect(toList());
    }

    private List<Result> getResults(
            @NonNull LdbcQuery1 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState
    ) {
        final long personId = operation.getPersonIdQ1();
        final String personFirstName = operation.getFirstName();
        final int limit = operation.getLimit();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.withSack(0).V().has('Person','id'," + personId + ")." +
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
                "select('university','studyFrom','country').by(valueMap('name')).by(valueMap()).fold()).fold())";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery1 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final List<LdbcQuery1Result> endResult = getResults(operation, dbConnectionState).stream()
                .map(r -> r.get(List.class))
                .map(l -> {
                    final Map<String, Object> person = toMap(l.get(0));
                    final List<String> emailList = parsePropertyStringList(person, "email");
                    final List<String> speaksList = parsePropertyStringList(person, "language");

                    return new LdbcQuery1Result(
                            parseId(person),
                            parseLastName(person),
                            parseIntValue(l.get(2)),
                            parsePersonBirthday(person),
                            parseCreationDate(person),
                            parsePersonGender(person),
                            parsePersonBrowserUsed(person),
                            parsePersonLocationIP(person),
                            emailList,
                            speaksList,
                            parseStringValue(toMap(l.get(1)), "name"),
                            getUniversities(l.get(4)),
                            getCompanies(l.get(3))
                    );
                })
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
