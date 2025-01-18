package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.BulkLogger;
import com.jackwaudby.ldbcimplementations.CompleteLoader;
import com.jackwaudby.ldbcimplementations.GraphCsvReader;
import com.jackwaudby.ldbcimplementations.csv.CsvItem;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadUtils.*;
import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static java.lang.Long.parseLong;

/**
 * This script provides a method for bulk loading edges.
 */
@Slf4j
@UtilityClass
public final class BulkLoadEdges {
    private static final Set<String> PROPERTIES_INTEGER = Set.of("classYear", "workFrom");

    public static void bulkLoadEdges(
            @NonNull String pathToData,
            @NonNull JanusGraph graph,
            @NonNull GraphTraversalSource g
    ) {
        final Set<String> vertexFilePaths = getVertexFilePaths(pathToData);
        final Set<String> vertexFilePropertiesPaths = getVertexPropertiesFilePaths(pathToData);
        final File dataDirectory = new File(pathToData);
        final File[] filesInDataDirectory = dataDirectory.listFiles();

        if (filesInDataDirectory == null) {
            CompleteLoader.getLogger().error("Supplied path is not a directory");
        }

        final BulkLogger bulkLogger = new BulkLogger();

        final Consumer<CsvItem> csvItemConsumer = i -> {
            final String edgeTail = getVertexOrEdgePart(i.getCleanFileName()[0]);
            final String edgeHead = getVertexOrEdgePart(i.getCleanFileName()[2]);
            final String edgeLabel = i.getCleanFileName()[1];

            final GraphTraversal<Vertex, Edge> traversal = g.V()
                    .has(edgeHead, "id", parseLong(i.getRecord().get(1)))
                    .as("a")
                    .V().has(edgeTail, "id", parseLong(i.getRecord().get(0)))
                    .addE(edgeLabel);

            setProperty(i.getRecord(), i.getHeader(), traversal);

            try {
                traversal.to("a").next();
            } catch (NoSuchElementException e) {
                log.error("Unexpected error", e);
            }

            bulkLogger.registerItem(getEdgeKey(edgeTail, edgeHead, edgeLabel));

            graph.tx().commit();
        };

        try (final GraphCsvReader graphCsvReader = new GraphCsvReader(csvItemConsumer)) {
            for (File file : filesInDataDirectory) {
                if (fileNotSuitable(file)
                        || vertexFilePaths.contains(file.toString())
                        || vertexFilePropertiesPaths.contains(file.toString())) {
                    continue;
                }

                final String[] cleanFileName = extractLabels(file.toString(), pathToData);

                if (cleanFileName.length != 3) {
                    throw new IllegalArgumentException(String.format("Invalid edge file name '%s'", file));
                }

                final String edgeTail = getVertexOrEdgePart(cleanFileName[0]);
                final String edgeHead = getVertexOrEdgePart(cleanFileName[2]);
                final String edgeLabel = cleanFileName[1];

                CompleteLoader.getLogger().info("Adding Edge: {}}", getEdgeKey(edgeTail, edgeHead, edgeLabel));

                bulkLogger.registerItemsToAdd(getEdgeKey(edgeTail, edgeHead, edgeLabel), lineCount(file));

                graphCsvReader.read(file, cleanFileName);
            }
        }

        bulkLogger.log();

        graph.tx().commit();
    }

    private static void setProperty(
            @NonNull CSVRecord record,
            @NonNull CSVRecord header,
            @NonNull GraphTraversal<Vertex, Edge> traversal
    ) {
        if (header.size() < 3) {
            return;
        }

        final String edgePropertyKey = header.get(2);

        if (PROPERTIES_INTEGER.contains(edgePropertyKey)) {
            traversal.property(edgePropertyKey, parseLong(record.get(2)));
        } else {
            traversal.property(edgePropertyKey, formatDatetime(record.get(2)).getTime());
        }
    }

    private static String getEdgeKey(@NonNull String tail, @NonNull String head, @NonNull String label) {
        return String.format("(%s)-[:%s]->(%s)", tail, label, head);
    }
}
