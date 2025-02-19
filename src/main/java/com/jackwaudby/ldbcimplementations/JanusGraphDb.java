package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.queryhandlers.*;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Slf4j
public final class JanusGraphDb extends Db {

    private JanusGraphConnectionState connectionState = null;

    /**
     * Get JanusGraph connection state
     *
     * @return JanusGraph connection state
     */
    @Override
    protected JanusGraphConnectionState getConnectionState() {
        return connectionState;
    }

    /**
     * Called before the benchmark is run. Note, OperationHandler implementations must be registered here.
     *
     * @param properties map of configuration properties
     * @throws DbException problem with SUT
     */
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {

        final String connectionUrl = properties.get("url");
        connectionState = new JanusGraphConnectionState(connectionUrl);

        registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
    }

    /**
     * Called after benchmark has completed.
     */
    @Override
    public void onClose() {
        connectionState.janusGraphClient.closeClient(); // perform clean up
    }

    @SuppressWarnings("unused")
    List<Result> execute(String queryString) {
        return connectionState.getClient().execute(queryString);
    }

    /**
     * Static nested class that creates a JanusGraph client
     */
    public static class JanusGraphClient {
        private final Cluster cluster;
        private final Client client;

        public JanusGraphClient(@Nullable String connectionUrl) {
            this.cluster = Cluster.build(connectionUrl).create();
            this.client = cluster.connect();
        }

        /**
         * Executes a Gremlin query against JanusGraph
         *
         * @return HTTP response message
         */
        @SneakyThrows
        public List<Result> execute(@NonNull String queryString) {
            final ResultSet result = client.submit(queryString);
            return result.all().get();
        }

        /**
         * Close client HTTP connection
         */
        void closeClient() {
            cluster.close();
        }
    }

    /**
     * Static nested class that implements DbConnectionState
     * Used to share any state that needs to be shared between OperationHandler instances.
     */
    public static class JanusGraphConnectionState extends DbConnectionState {

        private final JanusGraphClient janusGraphClient;

        /**
         * Creates a JanusGraph connection state
         *
         * @param connectionUrl connection url to JanusGraph server
         */
        private JanusGraphConnectionState(@Nullable String connectionUrl) {
            janusGraphClient = new JanusGraphClient(connectionUrl);
        }

        /**
         * Returns the JanusGraph client
         *
         * @return JanusGraph client
         */
        public JanusGraphClient getClient() {
            return janusGraphClient;
        }

        /**
         * Closes the JanusGraph client
         */
        @Override
        public void close() {
            janusGraphClient.closeClient();
        }
    }
}
