package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

public class LdbcQuery7Handler extends GremlinHandler implements OperationHandler<LdbcQuery7, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery7 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState
    ) {
        final long personId = operation.getPersonIdQ7();
        final long limit = operation.getLimit();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").in('hasCreator').as('message')." +
                "order().by('creationDate',desc).by('id',asc)" +
                "inE('likes').as('like')." +
                "order().by('creationDate',desc).outV().as('person').dedup().limit(" + limit + ")" +
                "choose(both('knows').has('Person','id'," + personId + "),constant(false),constant(true)).as('isNew')." +
                "select('person','message','isNew','like')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','content','imageFile','creationDate'))." +
                "by(fold())." +
                "by(valueMap('creationDate'))";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery7 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final List<LdbcQuery7Result> endResult = getResults(operation, dbConnectionState).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> {
                    final long likeCreationDate = parseLongValue(r, "likeCreationDate");
                    final long messageCreationDate = parseMessageCreationDate(r);

                    return new LdbcQuery7Result(
                            parsePersonId(r),
                            parsePersonFirstName(r),
                            parsePersonLastName(r),
                            parseLikeCreationDate(r),
                            parseMessageId(r),
                            parsePersonContent(r),
                            (int) ((likeCreationDate - messageCreationDate) / 60000),
                            parseBooleanValue(r, "isNew")
                    );
                })
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
