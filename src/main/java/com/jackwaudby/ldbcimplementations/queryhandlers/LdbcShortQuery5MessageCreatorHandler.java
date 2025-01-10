package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreator;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery5MessageCreatorResult;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message, retrieve its author and their ID, firstName and lastName.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery5MessageCreatorHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery5MessageCreator, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long messageId = operation.getMessageIdCreator();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result=g.V().has('Comment','id'," + messageId + ").fold()" +
                ".coalesce(unfold(),V().has('Post','id'," + messageId + "))" +
                ".out('hasCreator').valueMap('id','firstName','lastName').toList();[];" +
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

        final Map<String, Object> person = resultToMap(response.get(0));
        final long personId = parseId(person);
        final String firstName = parseStringValue(person, "firstName");
        final String lastName = parseStringValue(person, "lastName");

        final LdbcShortQuery5MessageCreatorResult queryResult = new LdbcShortQuery5MessageCreatorResult(
                personId,
                firstName,
                lastName
        );

        resultReporter.report(0, queryResult, operation);
    }
}
