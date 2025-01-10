package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery2Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

public class LdbcQuery2Handler extends GremlinHandler implements OperationHandler<LdbcQuery2, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery2 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState
    ) {
        final long personId = operation.getPersonIdQ2();
        final long date = operation.getMaxDate().getTime();
        final int limit = operation.getLimit();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").both('knows').as('person')." +
                "in('hasCreator').has('creationDate',lte(new Date(" + date + ")))." +
                "order().by('creationDate',desc).by('id',asc).limit(" + limit + ").as('message')." +
                "select('person','message')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','imageFile','content','creationDate'))";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery2 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final List<LdbcQuery2Result> endResult = getResults(operation, dbConnectionState).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcQuery2Result(
                        parsePersonId(r),
                        parsePersonFirstName(r),
                        parsePersonLastName(r),
                        parseMessageId(r),
                        parsePersonContent(r),
                        parseMessageCreationDate(r)
                ))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
