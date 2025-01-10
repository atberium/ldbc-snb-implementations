package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery8Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

public class LdbcQuery8Handler extends GremlinHandler implements OperationHandler<LdbcQuery8, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery8 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState
    ) {
        final long personId = operation.getPersonIdQ8();
        final int limit = operation.getLimit();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ")." +
                "in('hasCreator')." +
                "in('replyOf').as('message')." +
                "order().by('creationDate',desc).by('id',asc).limit(" + limit + ")." +
                "out('hasCreator').as('person')." +
                "select('message','person')." +
                "by(valueMap('id','creationDate','content'))." +
                "by(valueMap('id','firstName','lastName'))";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery8 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final List<LdbcQuery8Result> endResult = getResults(operation, dbConnectionState).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcQuery8Result(
                        parsePersonId(r),
                        parsePersonFirstName(r),
                        parsePersonLastName(r),
                        parseMessageCreationDate(r),
                        parseMessageId(r),
                        parseMessageContent(r)
                ))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
