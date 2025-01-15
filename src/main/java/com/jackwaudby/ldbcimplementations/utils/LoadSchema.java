package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.Date;

@UtilityClass
public class LoadSchema {

    /**
     * Loads schema
     *
     * @param graph JanusGraph instance
     */
    public static void loadSchema(@NonNull JanusGraph graph) {

        final JanusGraphManagement mgmt = graph.openManagement();

        try {
            mgmt.makeVertexLabel("Place").make();
            mgmt.makeVertexLabel("Comment").make();
            mgmt.makeVertexLabel("Forum").make();
            mgmt.makeVertexLabel("Person").make();
            mgmt.makeVertexLabel("Post").make();
            mgmt.makeVertexLabel("Tag").make();
            mgmt.makeVertexLabel("TagClass").make();
            mgmt.makeVertexLabel("Organisation").make();
            mgmt.makeEdgeLabel("containerOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasCreator").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasInterest").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasMember").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasModerator").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasTag").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isLocatedIn").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isPartOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isSubclassOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("likes").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("replyOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("studyAt").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("workAt").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasType").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makePropertyKey("id").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("title").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("creationDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("browserUsed").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("locationIP").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("content").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("length").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("url").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("firstName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("lastName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("gender").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("birthday").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("email").dataType(String.class).cardinality(Cardinality.LIST).make();
            mgmt.makePropertyKey("language").dataType(String.class).cardinality(Cardinality.LIST).make();
            mgmt.makePropertyKey("imageFile").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("type").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("joinDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("classYear").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
            mgmt.makePropertyKey("workFrom").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();

            mgmt.commit();

        } catch (SchemaViolationException e) {
            CompleteLoader.getLogger().error("Schema may already be defined", e);
        }
    }
}
