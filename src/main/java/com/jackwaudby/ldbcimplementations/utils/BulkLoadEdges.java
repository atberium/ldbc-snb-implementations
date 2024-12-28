package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadUtils.*;
import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;

/**
 * This script provides a method for bulk loading edges.
 */
@Slf4j
@UtilityClass
public final class BulkLoadEdges {

    public static void bulkLoadEdges(
            @NonNull String pathToData,
            @NonNull JanusGraph graph,
            @NonNull GraphTraversalSource g,
            @NonNull Map<String, Object> ldbcIdToJanusGraphId
    ) {
        final List<String> integerProperties = new ArrayList<>();
        integerProperties.add("classYear");
        integerProperties.add("workFrom");

        final Set<String> vertexFilePaths = getVertexFilePaths(pathToData);
        final File dataDirectory = new File(pathToData);
        final File[] filesInDataDirectory = dataDirectory.listFiles();

        if (filesInDataDirectory != null) {
            for (File file : filesInDataDirectory) {
                if (fileNotSuitable(file) || fileContainsVertices(file, vertexFilePaths)) {
                    continue;
                }
                final String[] cleanFileName = extractLabels(file.toString(), pathToData);

                if (cleanFileName.length != 3) {
                    throw new IllegalArgumentException(String.format("Invalid edge file name '%s'", file));
                }

                final String edgeTail = getVertexOrEdgePart(cleanFileName[0]);
                final String edgeHead = getVertexOrEdgePart(cleanFileName[2]);
                final String edgeLabel = cleanFileName[1];

                CompleteLoader.getLogger().info("Adding Edge: ({})-[:{}]->({})", edgeTail, edgeLabel, edgeHead);

                final int elementsToAdd = lineCount(file);

                Reader in;
                try {
                    in = new FileReader(file);
                    Iterable<CSVRecord> records;
                    try {
                        records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);
                        final CSVRecord header = records.iterator().next();

                        final List<String> edgeInfo = new ArrayList<>();
                        for (int i = 0; i < header.size(); i++) {
                            edgeInfo.add(header.get(i));
                        }

                        int elementsAdded = 0;
                        String startId;
                        String endId;
                        if (edgeInfo.size() == 3) {

                            final String edgePropertyKey = edgeInfo.get(2);

                            for (CSVRecord record : records) {

                                elementsAdded = elementsAdded + 1;
                                startId = record.get(0) + edgeTail;
                                endId = record.get(1) + edgeHead;

                                if (integerProperties.contains(edgePropertyKey)) {

                                    final long edgePropertyValue = Long.parseLong(record.get(2));

                                    g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                            .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                            .addE(edgeLabel)
                                            .property(edgePropertyKey, edgePropertyValue)
                                            .to("a")
                                            .next();

                                } else {
                                    final Date edgePropertyValue = FORMAT_DATETIME.parse(record.get(2));

                                    g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                            .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                            .addE(edgeLabel)
                                            .property(edgePropertyKey, edgePropertyValue.getTime())
                                            .to("a")
                                            .next();

                                }
                            }
                        } else {
                            for (CSVRecord record : records) {
                                elementsAdded = elementsAdded + 1;

                                startId = record.get(0) + edgeTail;
                                endId = record.get(1) + edgeHead;

                                g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                        .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                        .addE(edgeLabel)
                                        .to("a")
                                        .next();

                            }
                        }

                        graph.tx().commit();

                        CompleteLoader.getLogger().info("{}/{}", elementsAdded, elementsToAdd);
                    } catch (Exception e) {
                        log.error("Unexpected error", e);
                    }
                } catch (FileNotFoundException e) {
                    log.error("Unexpected error", e);
                }
            }
        }
        graph.tx().commit();
    }
}
