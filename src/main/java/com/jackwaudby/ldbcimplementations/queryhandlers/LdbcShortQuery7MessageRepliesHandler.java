package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageReplies;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery7MessageRepliesResult;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;
import static java.lang.Boolean.parseBoolean;
import static java.util.stream.Collectors.toList;

/**
 * Given a Message, retrieve the (1-hop) Comments that reply to it.
 * In addition, return a boolean flag knows indicating if the author of the reply knows the author of the original message.
 * If author is same as original author, return false for knows flag.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery7MessageRepliesHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery7MessageReplies, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long messageId = operation.getMessageRepliesId();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "originalMessage=g.V().has('Post','id'," + messageId + ").fold().coalesce(unfold(),V().has('Comment','id'," + messageId + ")).next();[];" +
                "originalAuthor=g.V(originalMessage).out('hasCreator').next();[];" +
                "result=g.V(originalMessage).as('originalMessage').in('replyOf').as('comment')." +
                "order().by(select('comment').by('creationDate'),desc).by(select('comment').by('id'),asc)." +
                "out('hasCreator').as('replyAuthor')." +
                "choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows')." +
                "select('comment','replyAuthor','knows')." +
                "by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by(fold())." +
                "map{it -> [" +
                "commentId:it.get().get('comment').get('id')," +
                "commentContent:it.get().get('comment').get('content')," +
                "commentCreationDate:it.get().get('comment').get('creationDate')," +
                "replyAuthorId:it.get().get('replyAuthor').get('id')," +
                "replyAuthorFirstName:it.get().get('replyAuthor').get('firstName')," +
                "replyAuthorLastName:it.get().get('replyAuthor').get('lastName')," +
                "replyAuthorKnowsOriginalMessageAuthor:it.get().get('knows')" +
                "]};[];" +
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

        final List<LdbcShortQuery7MessageRepliesResult> queryResults = response.stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcShortQuery7MessageRepliesResult(
                        parseLongValue(r, "commentId"),
                        parseStringValue(r, "commentContent"),
                        parsePersonCommentCreationDate(r),
                        parseLongValue(r, "replyAuthorId"),
                        parseStringValue(r, "replyAuthorFirstName"),
                        parseStringValue(r, "replyAuthorLastName"),
                        parseBoolean(parsePropertyStringList(r, "replyAuthorKnowsOriginalMessageAuthor").get(0))
                ))
                .collect(toList());

        log.info("Number of results: {}", queryResults.size());

        resultReporter.report(0, queryResults, operation);
    }
}
