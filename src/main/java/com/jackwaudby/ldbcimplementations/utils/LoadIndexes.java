package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import com.jackwaudby.ldbcimplementations.Index;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;

@Slf4j
@UtilityClass
public class LoadIndexes {
    /**
     * Loads indexes
     *
     * @param graph JanusGraph instance
     */
    public static void loadIndexes(@NonNull JanusGraph graph) {

        final JanusGraphManagement mgmt = graph.openManagement();

        try {
            for (Index index : Index.values()) {
                log.info("Creating index {} for label {}", index, index.getLabel());

                mgmt.buildIndex(index.toString(), Vertex.class)
                        .addKey(mgmt.getPropertyKey("id"))
                        .indexOnly(mgmt.getVertexLabel(index.getLabel()))
                        .unique()
                        .buildCompositeIndex();
            }

            mgmt.commit();

        } catch (SchemaViolationException e) {
            CompleteLoader.getLogger().error("Indexes may already be defined", e);
        }
    }
}
