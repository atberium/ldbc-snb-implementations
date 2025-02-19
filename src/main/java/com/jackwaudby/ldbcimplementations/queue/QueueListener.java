package com.jackwaudby.ldbcimplementations.queue;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static java.util.concurrent.Executors.newFixedThreadPool;

@Slf4j
public final class QueueListener<T> implements AutoCloseable {
    private final ExecutorService executor;
    private final List<Future<?>> futures = new ArrayList<>();

    public QueueListener(@NonNull BlockingQueue<T> queue, @NonNull Consumer<T> consumer, int poolSize) {
        this.executor = newFixedThreadPool(poolSize);

        for (int i = 0; i < poolSize; i++) {
            futures.add(executor.submit(() -> execute(queue, consumer)));
        }
    }

    @Override
    public void close() {
        futures.forEach(f -> f.cancel(true));

        executor.shutdown();
    }

    private void execute(@NonNull BlockingQueue<T> queue, @NonNull Consumer<T> consumer) {
        log.info("Listener executor started");

        while (!executor.isShutdown()) {
            try {
                consumer.accept(queue.take());
            } catch (InterruptedException e) {
                break;
            }
        }

        log.info("Listener executor finished");
    }
}
