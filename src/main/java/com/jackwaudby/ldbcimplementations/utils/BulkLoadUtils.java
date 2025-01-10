package com.jackwaudby.ldbcimplementations.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jackwaudby.ldbcimplementations.utils.JanusGraphUtils.getValidVertexFiles;
import static java.util.TimeZone.getTimeZone;

@UtilityClass
public class BulkLoadUtils {
    public static final DateFormat FORMAT_DATE = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat FORMAT_DATETIME = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final String WORD_DELIMITER = ";";
    private static final String TAG_CLASS = "Tagclass";
    private static final String SUITABLE_FILE_SUFFIX = ".csv";

    static {
        FORMAT_DATE.setTimeZone(getTimeZone("GMT"));
    }

    public static Set<String> getVertexFilePaths(@NonNull String pathToData) {
        return Stream.of(getValidVertexFiles().split(","))
                .map(f -> pathToData + f)
                .collect(Collectors.toSet());
    }

    public static boolean fileNotSuitable(@NonNull File file) {
        final String filePath = file.toString();

        return !file.toString().endsWith(SUITABLE_FILE_SUFFIX) || filePath.contains("update");
    }

    public static boolean fileContainsVertices(@NonNull File file, @NonNull Set<String> suitablePaths) {
        return suitablePaths.contains(file.toString());
    }

    public static String getVertexOrEdgePart(@NonNull String filenamePart) {
        return tagClassFix(filenamePart.substring(0, 1).toUpperCase() + filenamePart.substring(1));
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
