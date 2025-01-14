package com.jackwaudby.ldbcimplementations.queue;

import java.util.stream.Stream;

public interface Producer<T> {
    Stream<T> produce();
}
