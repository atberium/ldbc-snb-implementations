package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery9Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

public class LdbcQuery9Handler extends GremlinHandler implements OperationHandler<LdbcQuery9, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery9 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState
    ) {
        final long personId = operation.getPersonIdQ9();
        final long date = operation.getMaxDate().getTime();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ")." +
                "repeat(both('knows').simplePath()).emit().times(2).hasLabel('Person').dedup().as('person')." +
                "in('hasCreator').has('creationDate',lt(new Date(" + date + ")))." +
                "order().by('creationDate',desc).by('id',asc).limit(20).as('message')." +
                "select('person','message').by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','creationDate','content','imageFile'))";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery9 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final List<LdbcQuery9Result> endResult = getResults(operation, dbConnectionState).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcQuery9Result(
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
