package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContent;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery4MessageContentResult;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message ID, retrieve its content or imagefile and creation date.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery4MessageContentHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery4MessageContent, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long messageId = operation.getMessageIdContent();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Comment','id', " + messageId + ").fold()" +
                ".coalesce(unfold(),V().has('Post','id'," + messageId + "))" +
                ".valueMap('creationDate','content','imageFile').toList();[];" +
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

        final Map<String, Object> message = resultToMap(response.get(0));
        final LdbcShortQuery4MessageContentResult queryResult = new LdbcShortQuery4MessageContentResult(
                parseContent(message, "imageFile", "content"),
                parseCreationDate(message)
        );

        resultReporter.report(0, queryResult, operation);
    }
}
