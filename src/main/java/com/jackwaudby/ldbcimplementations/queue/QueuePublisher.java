package com.jackwaudby.ldbcimplementations.queue;

import lombok.NonNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class QueuePublisher<T> implements AutoCloseable {
    private final BlockingQueue<T> queue;
    private final ExecutorService executor;

    public QueuePublisher(@NonNull BlockingQueue<T> queue, int poolSize) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("Pool size must be greater than 0");
        }

        this.queue = queue;
        this.executor = newFixedThreadPool(poolSize);
    }

    public Future<?> publish(@NonNull Producer<T> producer) {
        return executor.submit(() -> producer.produce().forEach(this::put));
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private void put(T element) {
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
