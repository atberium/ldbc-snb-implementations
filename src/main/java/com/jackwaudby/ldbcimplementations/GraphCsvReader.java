package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.csv.CsvItem;
import com.jackwaudby.ldbcimplementations.queue.QueueListener;
import com.jackwaudby.ldbcimplementations.queue.QueuePublisher;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public final class GraphCsvReader implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 50000;
    private static final int DEFAULT_EXECUTORS_COUNT = getRuntime().availableProcessors() - 1;

    private final BlockingQueue<CsvItem> queue;
    private final QueueListener<CsvItem> listener;
    private final QueuePublisher<CsvItem> publisher;
    private final List<Future<?>> futures = new ArrayList<>();

    public GraphCsvReader(@NonNull Consumer<CsvItem> consumer) {
        this(consumer, DEFAULT_EXECUTORS_COUNT);
    }

    public GraphCsvReader(@NonNull Consumer<CsvItem> consumer, int poolSize) {
        this.queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
        this.listener = new QueueListener<>(queue, consumer, poolSize);
        this.publisher = new QueuePublisher<>(queue, poolSize);
    }

    public GraphCsvReader(@NonNull Consumer<CsvItem> consumer, int poolSize, int queueCapacity) {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.listener = new QueueListener<>(queue, consumer, poolSize);
        this.publisher = new QueuePublisher<>(queue, poolSize);
    }

    @SneakyThrows
    private static void get(@NonNull Future<?> future) {
        future.get();
    }

    public void read(@NonNull File csvFile, @NonNull String[] cleanFileName) {
        futures.add(publisher.publish(new CsvFileProducer(csvFile, cleanFileName)));
    }

    @Override
    public void close() {
        log.info("Wait until all files produced");
        futures.forEach(GraphCsvReader::get);

        log.info("Wait until all records consumed");
        while (!queue.isEmpty()) {
            sleepUninterruptibly(1, SECONDS);
        }

        log.info("Close listener");
        listener.close();

        log.info("Close publisher");
        publisher.close();
    }
}
