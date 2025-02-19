package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery6Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.toMap;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;

public class LdbcQuery6Handler extends GremlinHandler implements OperationHandler<LdbcQuery6, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    @SuppressWarnings("unchecked")
    public void executeOperation(LdbcQuery6 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long personId = operation.getPersonIdQ6();
        final String tagName = operation.getTagName();
        final int limit = operation.getLimit();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "in('hasCreator').hasLabel('Post')." +
                "where(out('hasTag').has('Tag','name','" + tagName + "'))." +
                "out('hasTag').has('Tag','name',neq('" + tagName + "'))." +
                "groupCount().by('name').order(local).by(values,desc).by(keys,asc).unfold().limit(" + limit + ").fold()";

        final List<LdbcQuery6Result> endResult = request(client, queryString).stream()
                .flatMap(r -> ((List<Object>) r.get(List.class)).stream())
                .flatMap(o -> toMap(o).entrySet().stream())
                .map(e -> new LdbcQuery6Result(e.getKey(), parseInt(e.getValue().toString())))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
