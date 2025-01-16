package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.BulkLogger;
import com.jackwaudby.ldbcimplementations.CompleteLoader;
import com.jackwaudby.ldbcimplementations.GraphCsvReader;
import com.jackwaudby.ldbcimplementations.csv.CsvItem;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadUtils.*;
import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.list;

/**
 * This script provides a method for loading vertices properties.
 */
@Slf4j
@UtilityClass
public class BulkLoadVerticesProperties {
    public static void bulkLoadProperties(
            @NonNull String pathToData,
            @NonNull JanusGraph graph,
            @NonNull GraphTraversalSource g,
            @NonNull Map<String, Object> ldbcIdToJanusGraphId
    ) {
        final Set<String> vertexPropertiesFilePaths = getVertexPropertiesFilePaths(pathToData);
        final File dataDirectory = new File(pathToData);
        final File[] filesInDataDirectory = dataDirectory.listFiles();

        if (filesInDataDirectory == null) {
            CompleteLoader.getLogger().error("Supplied path is not a directory");
        }

        final BulkLogger bulkLogger = new BulkLogger();

        final Consumer<CsvItem> csvItemConsumer = i -> {
            final String vertexLabel = getLabel(i.getCleanFileName());
            final Object vertexId = ldbcIdToJanusGraphId.get(i.getRecord().get(0) + vertexLabel);

            g.V(vertexId).property(list, i.getHeader().get(1), i.getRecord().get(1)).next();

            bulkLogger.registerItem(vertexLabel);

            graph.tx().commit();
        };

        try (final GraphCsvReader graphCsvReader = new GraphCsvReader(csvItemConsumer)) {
            for (final File file : filesInDataDirectory) {
                if (fileNotSuitable(file) || !vertexPropertiesFilePaths.contains(file.toString())) {
                    continue;
                }

                final String[] cleanFileName = extractLabels(file.toString(), pathToData);

                if (cleanFileName.length != 3) {
                    throw new IllegalArgumentException(String.format("Invalid vertex properties file name '%s'", file));
                }

                final String vertexLabel = getLabel(cleanFileName);

                CompleteLoader.getLogger().info("Adding Vertex properties: ({}.{})", vertexLabel, cleanFileName[1]);

                bulkLogger.registerItemsToAdd(vertexLabel, lineCount(file));

                graphCsvReader.read(file, cleanFileName);
            }
        }

        bulkLogger.log();

        graph.tx().commit();
    }
}
