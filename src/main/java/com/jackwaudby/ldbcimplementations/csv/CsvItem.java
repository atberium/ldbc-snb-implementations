package com.jackwaudby.ldbcimplementations.csv;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVRecord;

@RequiredArgsConstructor
@Getter
public class CsvItem {
    private final CSVRecord record;
    private final CSVRecord header;
    private final String[] cleanFileName;
}
