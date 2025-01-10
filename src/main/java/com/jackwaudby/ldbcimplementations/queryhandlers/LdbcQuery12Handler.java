package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery12Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

/**
 * Given a start Person, find the Comments that this Person’s friends made in reply to Posts.
 * Considering only those Comments that are immediate (1-hop) replies to Posts, not the transitive (multi-hop) case.
 * Only consider Posts with a Tag in a given TagClass or in a descendent of that TagClass.
 * Count the number of these reply Comments.
 * Collect the Tags that were attached to the Posts they replied to,
 * but only collect Tags with the given TagClass or with a descendant of that TagClass
 * Return Persons with at least one reply, the reply count, and the collection of Tags.
 */

public class LdbcQuery12Handler extends GremlinHandler implements OperationHandler<LdbcQuery12, JanusGraphDb.JanusGraphConnectionState> {

    private List<Result> getResults(
            @NonNull JanusGraphDb.JanusGraphConnectionState dbConnectionState,
            long personId,
            @NonNull String tagClassName
    ) {
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").both('knows')." +
                "  where(" +
                "    local(" +
                "      __.in('hasCreator')." +
                "        where(out('replyOf').hasLabel('Post').out('hasTag').out('hasType').has('name','" + tagClassName + "'))." +
                "        out('replyOf').out('hasTag').count().is(gt(0))))." +
                "  order()." +
                "    by(local(__.in('hasCreator')." +
                "      where(out('replyOf').hasLabel('Post').out('hasTag').out('hasType').has('name','" + tagClassName + "'))." +
                "      out('replyOf').out('hasTag').count()),desc)." +
                "  local(union(" +
                "    __.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag')." +
                "      where(out('hasType').has('name','" + tagClassName + "'))." +
                "      count().fold()," +
                "    valueMap('id','firstName','lastName').unfold()," +
                "    __.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag')." +
                "      where(out('hasType').has('name','" + tagClassName + "')).dedup()." +
                "      values('name').fold()" +
                "  ).fold())";

        return request(client, queryString);
    }

    @Override
    public void executeOperation(LdbcQuery12 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long personId = operation.getPersonIdQ12();
        final String tagClassName = operation.getTagClassName();

        final List<LdbcQuery12Result> endResult = getResults(dbConnectionState, personId, tagClassName).stream()
                .map(r -> r.get(List.class))
                .map(l -> new LdbcQuery12Result(
                        parseId(toMap(l.get(1))),
                        parseStringValue(toMap(l.get(2)), "firstName"),
                        parseStringValue(toMap(l.get(3)), "lastName"),
                        parsePropertyStringList(l.get(4)),
                        parseIntValue(l.get(0))
                ))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
