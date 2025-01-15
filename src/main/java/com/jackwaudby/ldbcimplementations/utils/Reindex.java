package com.jackwaudby.ldbcimplementations.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.core.JanusGraph;

import java.util.Collections;
import java.util.List;

import static org.janusgraph.core.util.ManagementUtil.reindexAndEnableIndices;

@Slf4j
@UtilityClass
public class Reindex {
    private static final int REINDEX_TIMEOUT_MILLIS = 600000;
    private static final List<String> INDEXES = List.of(
            "byPlaceId",
            "byCommentId",
            "byOrganisationId",
            "byForumId",
            "byPersonId",
            "byPostId",
            "byTagId",
            "byTagClassId"
    );

    /**
     * Loads indexes
     *
     * @param graph JanusGraph instance
     */
    public static void reindex(@NonNull JanusGraph graph) {
        reindexAndEnableIndices(graph, INDEXES, Collections.emptyMap(), REINDEX_TIMEOUT_MILLIS);
    }
}
