package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getPropertiesPath;

/**
 * This script provides basic graph statistics.
 */
@Slf4j
@UtilityClass
public class GraphStats {
    public static void main(String[] args) {
        final JanusGraph graph = JanusGraphFactory.open(getPropertiesPath());
        final GraphTraversalSource g = graph.traversal();
        elementCount(g);
        closeGraph(g);
        graph.close();
    }

    public static void elementCount(GraphTraversalSource g) {
        final long vertices = g.V().count().next();
        final long edges = g.E().count().next();

        log.info("Total Vertices: {}", vertices);
        log.info("Total Edges: {}", edges);


    }

}
