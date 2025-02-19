package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * This script provides a method that counts the number of lines in a file.
 */
@Slf4j
@UtilityClass
class LineCount {

    static int lineCount(File file) {
        Reader in;
        LineNumberReader lnr;
        int linenumber = 0;
        try {
            in = new FileReader(file);
            lnr = new LineNumberReader(in);
            while (lnr.readLine() != null) {
                linenumber++;
            }
        } catch (IOException e) {
            log.error("Unexpected error", e);
        }
        return (linenumber - 1);
    }
}
