package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.Index;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.janusgraph.core.JanusGraph;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.janusgraph.core.util.ManagementUtil.reindexAndEnableIndices;

@Slf4j
@UtilityClass
public class Reindex {
    private static final int REINDEX_TIMEOUT_MILLIS = 600000;
    private static final List<String> INDEXES = Stream.of(Index.values()).map(Enum::toString).collect(toList());

    /**
     * Loads indexes
     *
     * @param graph JanusGraph instance
     */
    public static void reindex(@NonNull JanusGraph graph) {
        reindexAndEnableIndices(graph, INDEXES, emptyMap(), REINDEX_TIMEOUT_MILLIS);
    }
}
