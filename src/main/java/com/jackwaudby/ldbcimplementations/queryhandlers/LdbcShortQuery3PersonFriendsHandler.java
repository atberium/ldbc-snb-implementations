package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriends;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery3PersonFriendsResult;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;
import static java.util.stream.Collectors.toList;

/**
 * Given a start Person, retrieve all of their friend's ID, firstName, lastName, and the date at which they became friends.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery3PersonFriendsHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery3PersonFriends, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long personId = operation.getPersonIdSQ3();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Person','id'," + personId + ").bothE('knows').as('friendshipCreationDate').otherV().as('friend')." +
                "order().by(select('friendshipCreationDate').by('creationDate'),desc)." +
                "by('id',asc).select('friendshipCreationDate','friend')." +
                "by(values('creationDate').fold()).by(valueMap('id','firstName','lastName')).toList();" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result";

        final List<Result> response = tryRequest(client, queryString, getTxnAttempts());

        if (response == null) {
            return;
        }

        final List<LdbcShortQuery3PersonFriendsResult> queryResultList = response.stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> {
                    final Map<String, Object> friend = toMap(r.get("friend"));

                    return new LdbcShortQuery3PersonFriendsResult(
                            parseId(friend),
                            parseStringValue(friend, "firstName"),
                            parseStringValue(friend, "lastName"),
                            parsePersonFriendshipCreationDate(r)
                    );
                })
                .collect(toList());

        resultReporter.report(0, queryResultList, operation);
    }
}
