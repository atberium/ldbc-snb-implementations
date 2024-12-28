package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.Environments;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import javax.annotation.Nullable;

import static com.jackwaudby.ldbcimplementations.Environments.*;
import static java.lang.System.getenv;
import static java.util.Optional.ofNullable;

@UtilityClass
public class JanusGraphUtils {
    private static final String DEFAULT_JANUS_GRAPH_PROPERTY = "conf/janusgraph-berkeleyje.properties";
    private static final String DEFAULT_JANUS_GRAPH_DATA = "data";
    private static final String DEFAULT_VALID_VERTEX_FILES = "comment_0_0.csv,forum_0_0.csv,person_0_0.csv," +
            "organisation_0_0.csv,place_0_0.csv," +
            "post_0_0.csv,tag_0_0.csv,tagclass_0_0.csv";

    public static String getPropertiesPath() {
        return getEnv(PATH_TO_JANUSGRAPH_PROPERTIES, DEFAULT_JANUS_GRAPH_PROPERTY);
    }

    public static String getDataPath() {
        return getEnv(PATH_TO_JANUSGRAPH_DATA, DEFAULT_JANUS_GRAPH_DATA);
    }

    public static String getValidVertexFiles() {
        return getEnv(VALID_VERTEX_FILES, DEFAULT_VALID_VERTEX_FILES);
    }

    private static String getEnv(@NonNull Environments environment, @Nullable String defaultValue) {
        return ofNullable(getenv(environment.toString())).orElse(defaultValue);
    }
}
