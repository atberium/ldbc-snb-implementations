package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery11Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

/**
 * Given a start Person, find that Person’s friends and friends of friends (excluding start Person)
 * who started Working in some Company in a given Country, before a given date (year).
 */
public class LdbcQuery11Handler extends GremlinHandler implements OperationHandler<LdbcQuery11, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery11 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState,
            long personId
    ) {
        final String countryName = operation.getCountryName();
        final int workFromYear = operation.getWorkFromYear();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ")." +
                "repeat(both('knows').simplePath()).emit().times(2).dedup().as('person')." +
                "outE('workAt').has('workFrom',lt(" + workFromYear + ")).as('organisationYear')." +
                "inV().as('organisation')." +
                "out('isLocatedIn').has('name','" + countryName + "')." +
                "order()." +
                "by(select('organisationYear').by('workFrom'))." +
                "by(select('person').by('id'))." +
                "by(select('organisation').by('name'),desc)." +
                "select('person','organisation','organisationYear')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('name')).by(valueMap('workFrom'))";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery11 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long personId = operation.getPersonIdQ11();
        final List<LdbcQuery11Result> endResult = getResults(operation, dbConnectionState, personId).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcQuery11Result(
                        parsePersonId(r),
                        parsePersonFirstName(r),
                        parsePersonLastName(r),
                        parseOrganizationName(r),
                        parseOrganizationYearWorkFrom(r)
                ))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
