package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery5Result;

import java.util.List;
import java.util.stream.Collectors;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Title: New groups
 * <p>
 * Description: Given a start Person, find the Forums which that Person’s friends and friends of friends
 * (excluding start Person) became Members of after a given date. For each forum find the number of Posts
 * that were created by any of these Persons. For each Forum and consider only those Persons which joined
 * that particular Forum after the given date.
 */
public class LdbcQuery5Handler extends GremlinHandler implements OperationHandler<LdbcQuery5, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery5 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long personId = operation.getPersonIdQ5();
        final long minDate = operation.getMinDate().getTime();
        final long limit = 20;

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup().sideEffect(store('a')).aggregate('friends')." +
                "inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().dedup().as('forums')." +
                "order().by(local(out('containerOf').match(__.as('post').out('hasCreator').where(within('friends')).as('friend')," +
                "__.as('post').in('containerOf').as('forum'),__.as('friend').inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().where(eq('forum')).as('forum')).count()),desc)." +
                "by('id').limit(" + limit + ").local(union(valueMap('title').unfold(),out('containerOf').match(__.as('post').out('hasCreator').where(within('a')).as('friend')," +
                "__.as('post').in('containerOf').as('forum'), __.as('friend').inE('hasMember').has('joinDate',gt(new Date(" + minDate + "))).outV().where(eq('forum')).as('forum')).count().fold()).fold())";

        final List<LdbcQuery5Result> endResult = request(client, queryString).stream()
                .map(r -> r.get(List.class))
                .map(l -> new LdbcQuery5Result(
                        parseStringValue(toMap(l.get(0)), "title"),
                        parseIntValue(l.get(1))
                ))
                .collect(Collectors.toList());

        resultReporter.report(0, endResult, operation);
    }

}
