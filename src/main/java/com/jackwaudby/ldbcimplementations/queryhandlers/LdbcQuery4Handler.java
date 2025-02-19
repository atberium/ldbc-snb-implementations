package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4Result;

import java.util.List;
import java.util.stream.Collectors;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.toMap;
import static java.lang.Integer.parseInt;

public class LdbcQuery4Handler extends GremlinHandler implements OperationHandler<LdbcQuery4, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull LdbcQuery4 operation,
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState,
            long personId
    ) {
        final long startDate = operation.getStartDate().getTime();
        final long duration = (operation.getDurationDays() * 24L * 60L * 60L * 1000L);
        final long endDate = startDate + duration;
        final long limit = operation.getLimit();


        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "g.V().has('Person','id'," + personId + ").both('knows').in('hasCreator')" +
                ".sideEffect(has('Post','creationDate',lt(new Date(" + startDate + ")))" +
                ".out('hasTag').aggregate('oldTags'))" +
                ".has('Post','creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))" +
                ".out('hasTag').where(without('oldTags')).order().by('name').group().by('name').by(count())" +
                ".order(local).by(values,desc).by(keys,asc).unfold().limit(" + limit + ").fold()";

        return request(client, queryString);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void executeOperation(LdbcQuery4 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long personId = operation.getPersonIdQ4();
        final List<LdbcQuery4Result> endResult = getResults(operation, dbConnectionState, personId).stream()
                .flatMap(r -> ((List<Object>) r.get(List.class)).stream())
                .flatMap(o -> toMap(o).entrySet().stream())
                .map(e -> new LdbcQuery4Result(e.getKey(), parseInt(e.getValue().toString())))
                .collect(Collectors.toList());

        resultReporter.report(0, endResult, operation);

    }
}
