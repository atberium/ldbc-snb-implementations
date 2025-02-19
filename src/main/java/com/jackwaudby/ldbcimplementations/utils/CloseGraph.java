package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * This script provides a method for closing a Graph Traversal Source.
 */
@Slf4j
@UtilityClass
public class CloseGraph {

    public static void closeGraph(GraphTraversalSource g) {
        try {
            g.close();
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }

}
