package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForum;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery6MessageForumResult;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message ID, retrieve the Forum that contains it (ID and title) and
 * retrieve the ID, firstName and lastName of the Person that moderates the Forum.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery6MessageForumHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery6MessageForum, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long messageId = operation.getMessageForumId();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result=g.V().has('Post','id'," + messageId + ").fold()." +
                "coalesce(unfold(),V().has('Comment','id'," + messageId + ")." +
                "repeat(out('replyOf').simplePath()).until(hasLabel('Post')))." +
                "in('containerOf').as('forum').out('hasModerator').as('moderator')." +
                "select('forum','moderator')." +
                "by(valueMap('id','title'))." +
                "by(valueMap('id','firstName','lastName')).toList();[];" +
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
        final Map<String, Object> forum = toMap(person.get("forum"));
        final long forumId = parseId(forum);
        final String forumTitle = parseStringValue(forum, "title");

        final Map<String, Object> moderator = toMap(person.get("moderator"));
        final long moderatorId = parseId(moderator);
        final String moderatorFirstName = parseStringValue(moderator, "firstName");
        final String moderatorLastName = parseStringValue(moderator, "lastName");

        final LdbcShortQuery6MessageForumResult queryResult = new LdbcShortQuery6MessageForumResult(
                forumId,
                forumTitle,
                moderatorId,
                moderatorFirstName,
                moderatorLastName
        );

        resultReporter.report(0, queryResult, operation);
    }
}
