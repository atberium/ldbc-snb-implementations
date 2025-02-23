package com.jackwaudby.ldbcimplementations;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadEdges.bulkLoadEdges;
import static com.jackwaudby.ldbcimplementations.utils.BulkLoadVertices.bulkLoadVertices;
import static com.jackwaudby.ldbcimplementations.utils.BulkLoadVerticesProperties.bulkLoadProperties;
import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getDataPath;
import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getPropertiesPath;
import static com.jackwaudby.ldbcimplementations.utils.LoadIndexes.loadIndexes;
import static com.jackwaudby.ldbcimplementations.utils.LoadSchema.loadSchema;
import static com.jackwaudby.ldbcimplementations.utils.Reindex.reindex;

/**
 * This script creates embedded connection with JanusGraph and loads schema, indexes and data.
 */
@Slf4j
@UtilityClass
public class CompleteLoader {
    public static void main(String[] args) {
        log.info("Loading Configuration:");

        final String pathToData = getDataPath();

        log.info("Opening JanusGraph connection");

        try (JanusGraph graph = JanusGraphFactory.open(getPropertiesPath())) {
            log.info("Creating Graph Traversal Source");
            final GraphTraversalSource g = graph.traversal();

            log.info("Loading Schema");
            loadSchema(graph);

            final JanusGraphManagement schema = graph.openManagement();
            log.info("Schema: {}", schema.printSchema());
            schema.commit();

            log.info("Loading Vertices");
            bulkLoadVertices(pathToData, graph, g);

            log.info("Loading Index");
            loadIndexes(graph);

            log.info("Reindex");
            reindex(graph);

            log.info("Loading Vertices Properties");
            bulkLoadProperties(pathToData, graph, g);

            log.info("Loading Edges");
            bulkLoadEdges(pathToData, graph, g);

            log.info("Closing Graph Traversal Source");
            closeGraph(g);
        }
    }

    public static Logger getLogger() {
        return log;
    }
}
