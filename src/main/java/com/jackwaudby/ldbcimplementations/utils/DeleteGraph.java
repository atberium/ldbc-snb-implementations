package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.GraphStats.elementCount;
import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getPropertiesPath;
import static java.lang.System.exit;

/**
 * This script drops a graph.
 */
@Slf4j
@UtilityClass
public class DeleteGraph {

    public static void main(String[] args) {

        final JanusGraph graph = JanusGraphFactory.open(getPropertiesPath());
        final GraphTraversalSource g = graph.traversal();

        elementCount(g);                 // get stats
        log.info("Dropping Graph");

        g.V().drop().iterate();                     // drop graph
        graph.tx().commit();                        // commit changes

        log.info("Graph Dropped");

        elementCount(g);                 // get stats
        closeGraph(g);                   // close graph
        exit(0);                      // close program
    }


}
