package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;

import javax.annotation.Nullable;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.getResultError;

@Slf4j
abstract class GremlinHandler {
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    @NonNull
    protected List<Result> request(@NonNull JanusGraphDb.JanusGraphClient client, @NonNull String query) {
        return client.execute(query);
    }

    @Nullable
    protected List<Result> tryRequest(@NonNull JanusGraphDb.JanusGraphClient client, @NonNull String query) {
        return tryRequest(client, query, DEFAULT_MAX_ATTEMPTS);
    }

    @Nullable
    public List<Result> tryRequest(
            @NonNull JanusGraphDb.JanusGraphClient client,
            @NonNull String query,
            int maxAttempts
    ) {
        int attempt = 0;

        while (attempt < maxAttempts) {
            log.info("Attempt {} of handler {}", attempt + 1, this.getClass().getSimpleName());

            final List<Result> response = request(client, query);
            final String error = getResultError(response);

            if (error == null) {
                return response;
            }

            log.error("Error occured in handler {}: {}", this.getClass().getSimpleName(), error);
            attempt++;
        }

        return null;
    }
}
