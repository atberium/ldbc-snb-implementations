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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.util.Set;
import java.util.function.Consumer;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadUtils.*;
import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * This script provides a method for loading vertices.
 */
@Slf4j
@UtilityClass
public class BulkLoadVertices {
    private static final Set<String> PROPERTIES_INTEGER = Set.of("length", "classYear", "workForm");
    private static final Set<String> PROPERTIES_DATETIME = Set.of("joinDate", "creationDate");

    public static void bulkLoadVertices(
            @NonNull String pathToData,
            @NonNull JanusGraph graph,
            @NonNull GraphTraversalSource g
    ) {
        final Set<String> vertexFilePaths = getVertexFilePaths(pathToData);
        final File dataDirectory = new File(pathToData);
        final File[] filesInDataDirectory = dataDirectory.listFiles();

        if (filesInDataDirectory == null) {
            CompleteLoader.getLogger().error("Supplied path is not a directory");
        }

        final BulkLogger bulkLogger = new BulkLogger();

        final Consumer<CsvItem> csvItemConsumer = i -> {
            final String vertexLabel = getLabel(i.getCleanFileName());
            final long ldbcId = parseLong(i.getRecord().get(0));
            final GraphTraversal<Vertex, Vertex> traversal = g.addV(vertexLabel)
                    .property(i.getHeader().get(0), ldbcId);

            addProperties(traversal, i.getRecord(), i.getHeader());

            traversal.next();

            bulkLogger.registerItem(vertexLabel);

            graph.tx().commit();
        };

        try (final GraphCsvReader graphCsvReader = new GraphCsvReader(csvItemConsumer)) {
            for (File file : filesInDataDirectory) {
                if (fileNotSuitable(file) || !vertexFilePaths.contains(file.toString())) {
                    continue;
                }

                final String[] cleanFileName = extractLabels(file.toString(), pathToData);

                if (cleanFileName.length != 1) {
                    throw new IllegalArgumentException(String.format("Invalid vertex file name '%s'", file));
                }

                final String vertexLabel = getLabel(cleanFileName);

                CompleteLoader.getLogger().info("Adding Vertex: ({})", vertexLabel);

                bulkLogger.registerItemsToAdd(vertexLabel, lineCount(file));

                graphCsvReader.read(file, cleanFileName);
            }
        }

        bulkLogger.log();

        graph.tx().commit();
    }

    private static void addProperties(
            @NonNull GraphTraversal<Vertex, Vertex> traversal,
            @NonNull CSVRecord record,
            @NonNull CSVRecord header
    ) {
        for (int i = 1; i < header.size(); i++) {

            final String columnValue = record.get(i);

            try {
                if (PROPERTIES_INTEGER.contains(header.get(i))) {
                    traversal.property(header.get(i), parseInt(columnValue));
                } else if (header.get(i).contentEquals("birthday")) {
                    traversal.property(header.get(i), formatDate(columnValue));
                } else if (PROPERTIES_DATETIME.contains(header.get(i))) {
                    traversal.property(header.get(i), formatDatetime(columnValue));
                } else {
                    traversal.property(header.get(i), columnValue);
                }
            } catch (Exception e) {
                log.error("Unexpected parse error for column value `{}` of record `{}`", columnValue, record, e);
            }
        }
    }
}
