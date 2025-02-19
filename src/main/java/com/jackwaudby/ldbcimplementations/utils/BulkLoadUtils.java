package com.jackwaudby.ldbcimplementations.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.File;
import java.util.Date;
import java.util.Set;
import java.util.stream.Stream;

import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getValidVertexFiles;
import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getValidVertexPropertiesFiles;
import static java.lang.Long.parseLong;
import static java.util.stream.Collectors.toSet;

@UtilityClass
public class BulkLoadUtils {
    private static final String TAG_CLASS = "Tagclass";
    private static final String SUITABLE_FILE_SUFFIX = ".csv";

    public static Set<String> getVertexFilePaths(@NonNull String pathToData) {
        return getFilePaths(getValidVertexFiles(), pathToData);
    }

    public static Set<String> getVertexPropertiesFilePaths(@NonNull String pathToData) {
        return getFilePaths(getValidVertexPropertiesFiles(), pathToData);
    }

    public static boolean fileNotSuitable(@NonNull File file) {
        final String filePath = file.toString();

        return !file.toString().endsWith(SUITABLE_FILE_SUFFIX) || filePath.contains("update");
    }

    public static String getVertexOrEdgePart(@NonNull String filenamePart) {
        return tagClassFix(filenamePart.substring(0, 1).toUpperCase() + filenamePart.substring(1));
    }

    public static Date formatDate(@NonNull String date) {
        return new Date(parseLong(date));
    }

    public static Date formatDatetime(@NonNull String datetime) {
        return new Date(parseLong(datetime));
    }

    public static String getLabel(@NonNull String[] cleanFileName) {
        return getVertexOrEdgePart(cleanFileName[0]);
    }

    private static Set<String> getFilePaths(@NonNull String filesString, @NonNull String pathToData) {
        return Stream.of(filesString.split(","))
                .map(f -> pathToData + f)
                .collect(toSet());
    }

    private static String tagClassFix(String s) {
        if (s.contentEquals(TAG_CLASS)) {
            return s.substring(0, 3) +
                    s.substring(3, 4).toUpperCase() +
                    s.substring(4);
        }

        return s;
    }
}
