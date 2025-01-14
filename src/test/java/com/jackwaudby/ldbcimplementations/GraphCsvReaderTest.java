package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.csv.CsvItem;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class GraphCsvReaderTest {
    @Test
    void shouldProperlyReadCsvFiles() {
        final Map<String, Integer> counts = new ConcurrentHashMap<>();
        final Set<String> threads = new HashSet<>();

        final Consumer<CsvItem> consumer = i -> {
            log.info("Clean file name: {}, record: {}", i.getCleanFileName(), i.getRecord().toString());
            counts.compute(i.getRecord().toString(), (k, v) -> v == null ? 1 : v + 1);
            threads.add(currentThread().getName());
            sleepUninterruptibly(10, MILLISECONDS);
        };

        try (final GraphCsvReader reader = new GraphCsvReader(consumer, 3, 2)) {
            final URL resource = getClass().getResource("/post_0_0.csv");

            assertNotNull(resource);

            final File file = new File(resource.getFile());

            reader.read(file, new String[]{"foo"});
            log.info("Started 1st");

            reader.read(file, new String[]{"bar"});
            log.info("Started 2nd");

            reader.read(file, new String[]{"baz"});
            log.info("Started 3rd");
        }

        assertEquals(147, counts.size());
        assertEquals(3, counts.values().stream().distinct().findFirst().orElse(0));
        assertEquals(3, threads.size());
    }
}
