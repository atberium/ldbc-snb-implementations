package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.parseIntValue;

/**
 * Title: Single shortest path
 * <p>
 * Description: Given two Persons, find the shortest path between these two Persons in the subgraph induced
 * by the Knows relationships.
 */
public class LdbcQuery13Handler extends GremlinHandler implements OperationHandler<LdbcQuery13, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState,
            long person1Id,
            long person2Id
    ) {
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + person1Id + ")." +
                "choose(" +
                "repeat(both('knows').dedup()).until(has('Person','id'," + person2Id + ")).limit(1).path().count(local).is(gt(0))," +
                "repeat(store('x').both('knows').where(without('x')).aggregate('x')).until(has('Person','id'," + person2Id + ")).limit(1).path().count(local)," +
                "constant(-1)).fold()";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery13 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long person1Id = operation.getPerson1IdQ13StartNode();
        final long person2Id = operation.getPerson2IdQ13EndNode();

        final List<Result> resultList = getResults(dbConnectionState, person1Id, person2Id);
        final int shortestPathLength = parseIntValue(resultList.get(0).get(List.class));

        final LdbcQuery13Result result = new LdbcQuery13Result(shortestPathLength != -1 ? shortestPathLength - 1 : shortestPathLength);
        resultReporter.report(0, result, operation);
    }
}
