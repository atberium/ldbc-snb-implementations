package com.jackwaudby.ldbcimplementations.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * This script provides a method for extracting labels from filenames.
 */
@UtilityClass
class ExtractLabels {
    static String[] extractLabels(@NonNull String stringToSplit, @NonNull String path) {
        return stringToSplit.replace(path, "")
                .replace("_0_0.csv", "")
                .split("_");
    }
}
