package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadUtils.*;
import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static org.apache.commons.csv.CSVFormat.DEFAULT;

/**
 * This script provides a method for loading vertices.
 */
@Slf4j
@UtilityClass
public class BulkLoadVertices {
    public static void bulkLoadVertices(
            @NonNull String pathToData,
            @NonNull JanusGraph graph,
            @NonNull GraphTraversalSource g,
            @NonNull Map<String, Object> ldbcIdToJanusGraphId
    ) {
        final List<String> integerProperties = new ArrayList<>();
        integerProperties.add("length");
        integerProperties.add("classYear");
        integerProperties.add("workForm");

        final List<String> dateTimeProperties = new ArrayList<>();
        dateTimeProperties.add("joinDate");
        dateTimeProperties.add("creationDate");

        final List<String> setProperties = new ArrayList<>();
        setProperties.add("language");
        setProperties.add("email");

        final Set<String> vertexFilePaths = getVertexFilePaths(pathToData);
        final File dataDirectory = new File(pathToData);
        final File[] filesInDataDirectory = dataDirectory.listFiles();

        if (filesInDataDirectory != null) {
            for (final File child : filesInDataDirectory) {
                if (fileNotSuitable(child) || !fileContainsVertices(child, vertexFilePaths)) {
                    continue;
                }

                final String[] cleanFileName = extractLabels(child.toString(), pathToData);

                if (cleanFileName.length != 1) {
                    throw new IllegalArgumentException(String.format("Invalid edge file name '%s'", child));
                }

                final String vertexLabel = getVertexOrEdgePart(cleanFileName[0]);

                CompleteLoader.getLogger().info("Adding Vertex: ({})", vertexLabel);

                final int elementsToAdd = lineCount(child);

                Reader in;
                try {
                    in = new FileReader(child);
                    Iterable<CSVRecord> records;
                    try {
                        records = DEFAULT.withDelimiter('|').parse(in);
                        final CSVRecord header = records.iterator().next();

                        int elementsAdded = 0;

                        for (CSVRecord record : records) {
                            final String compositeLdbcId = record.get(0) + vertexLabel;
                            final long ldbcId = Long.parseLong(record.get(0));

                            final Vertex v = g.addV(vertexLabel).property(header.get(0), ldbcId).next();

                            ldbcIdToJanusGraphId.put(compositeLdbcId, v.id());

                            elementsAdded = elementsAdded + 1;

                            for (int i = 1; i < header.size(); i++) {
                                if (integerProperties.contains(header.get(i))) {
                                    g.V(v).property(header.get(i), Integer.parseInt(record.get(i))).next();
                                } else if (header.get(i).contentEquals("birthday")) {
                                    g.V(v).property(header.get(i), FORMAT_DATE.parse(record.get(i))).next();
                                } else if (dateTimeProperties.contains(header.get(i))) {
                                    g.V(v).property(header.get(i), FORMAT_DATETIME.parse(record.get(i))).next();
                                } else if (setProperties.contains(header.get(i))) {
                                    final String[] setArray = record.get(i).split(WORD_DELIMITER);
                                    for (String s : setArray) {
                                        g.V(v).property(VertexProperty.Cardinality.list, header.get(i), s).next();
                                    }
                                } else {
                                    g.V(v).property(header.get(i), record.get(i)).next();
                                }
                            }
                            graph.tx().commit();
                        }

                        CompleteLoader.getLogger().info("{}/{}", elementsAdded, elementsToAdd);
                    } catch (Exception e) {
                        log.error("Unexpected error", e);
                    }
                } catch (IOException e) {
                    CompleteLoader.getLogger().error("Unexpected error", e);
                }
            }
        } else {
            CompleteLoader.getLogger().error("Supplied path is not a directory");
        }
        graph.tx().commit();
    }
}
