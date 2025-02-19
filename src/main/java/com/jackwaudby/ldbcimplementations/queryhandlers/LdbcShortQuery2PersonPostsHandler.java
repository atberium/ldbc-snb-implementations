package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPosts;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery2PersonPostsResult;

import java.util.List;
import java.util.Map;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;
import static java.util.stream.Collectors.toList;

/**
 * Given a Person, retrieve the last 10 messages created by that user.
 * For each message, return the message ID, content/imageFile and creationDate.
 * Then return the ID of the original post from its conversation and the author of that post, ID, firstName and lastName.
 * If the message is a post then the original post will be the same message.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery2PersonPostsHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery2PersonPosts, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long limit = operation.getLimit();
        final long personId = operation.getPersonIdSQ2();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result = " +
                "g.V().has('Person','id'," + personId + ").in('hasCreator')." +
                "order().by('creationDate',desc).by('id',desc).limit(" + limit + ").as('message')." +
                "local(choose(" +
                "hasLabel('Post')," +
                "identity().as('originalPost').out('hasCreator').as('originalAuthor')," +
                "repeat(out('replyOf').simplePath()).until(hasLabel('Post')).as('originalPost').out('hasCreator').as('originalAuthor')))." +
                "select('message','originalPost','originalAuthor')." +
                "by(valueMap('id','imageFile','content','creationDate'))." +
                "by(valueMap('id'))." +
                "by(valueMap('id','firstName','lastName')).toList();" +
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

        final List<LdbcShortQuery2PersonPostsResult> queryResultList = response.stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> {
                    final Map<String, Object> originalAuthor = toMap(r.get("originalAuthor"));

                    return new LdbcShortQuery2PersonPostsResult(
                            parseMessageId(r),
                            parseMessageContent(r),
                            parseMessageCreationDate(r),
                            parseId(toMap(r.get("originalPost"))),
                            parseId(originalAuthor),
                            parseFirstName(originalAuthor),
                            parseLastName(originalAuthor)
                    );
                })
                .collect(toList());

        resultReporter.report(0, queryResultList, operation);
    }
}
