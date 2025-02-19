package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcUpdate6AddPost;

import java.util.List;
import java.util.Map;

@Slf4j
public class LdbcUpdate6AddPostHandler extends GremlinHandler implements OperationHandler<LdbcUpdate6AddPost, JanusGraphDb.JanusGraphConnectionState> {

    private static final int TX_RETRIES = 5;

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long postId = operation.getPostId();
        final String imageFile = operation.getImageFile();
        final long creationDate = operation.getCreationDate().getTime();
        final String locationIp = operation.getLocationIp();
        final String browserUsed = operation.getBrowserUsed();
        final String language = operation.getLanguage();
        final String content = operation.getContent();
        final int length = operation.getLength();
        final long personId = operation.getAuthorPersonId();
        final long forumId = operation.getForumId();
        final long countryId = operation.getCountryId();
        final List<Long> tagIds = operation.getTagIds();

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "try {" +
                "p = g.addV('Post')" +
                ".property('id'," + postId + ")" +
                ".property('imageFile','" + imageFile + "')" +
                ".property('creationDate','" + creationDate + "')" +
                ".property('locationIP','" + locationIp + "')" +
                ".property('browserUsed','" + browserUsed + "')" +
                ".property('language','" + language + "')" +
                ".property('content','''" + content + "''')" +
                ".property('length','" + length + "').next();[];" +
                "g.V().has('Person', 'id'," + personId + ").as('person').V(p).addE('hasCreator').to('person').next();[];" +
                "g.V().has('Place', 'id'," + countryId + ").as('country').V(p).addE('isLocatedIn').to('country').next();[];" +
                "g.V(p).as('forum').V().has('Forum','id'," + forumId + ").addE('containerOf').to('forum').next();[];" +
                "tagid=" + tagIds.toString() + ";[];" +
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;";


        int attempts = 0;

        while (attempts < TX_RETRIES) {
            log.info("Attempt {}", attempts + 1);
            final List<Result> response = request(client, queryString);
            final Map<String, Object> result = GremlinResponseParsers.resultToMap(response.get(0));
            if (result.containsKey("query_error")) {
                attempts = attempts + 1;
                log.error("Query Error: {}", result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                attempts = attempts + 1;
                log.error("Gremlin Server Error: {}", result.get("http_error"));
            } else {
                log.info(result.get("query_outcome").toString());
                break;
            }
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
}
