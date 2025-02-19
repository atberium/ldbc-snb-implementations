package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.csv.CsvItem;
import com.jackwaudby.ldbcimplementations.queue.Producer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.commons.csv.CSVFormat.DEFAULT;

@Slf4j
@RequiredArgsConstructor
public final class CsvFileProducer implements Producer<CsvItem> {
    private static final char DELIMITER = '|';
    private final File file;
    private final String[] cleanFileName;

    @Override
    @SneakyThrows
    public Stream<CsvItem> produce() {
        final Reader in = new FileReader(file);
        final Iterable<CSVRecord> records = DEFAULT.withDelimiter(DELIMITER).parse(in);
        final CSVRecord header = records.iterator().next();

        return StreamSupport.stream(records.spliterator(), false).map(r -> new CsvItem(r, header, cleanFileName));
    }
}
