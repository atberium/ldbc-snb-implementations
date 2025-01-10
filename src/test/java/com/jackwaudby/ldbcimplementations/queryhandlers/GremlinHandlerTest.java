package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class GremlinHandlerTest {
    private Handler handler;
    @Mock
    private JanusGraphDb.JanusGraphClient client;

    @BeforeEach
    @SneakyThrows
    void setup() {
        try (AutoCloseable ignored = openMocks(this)) {
            handler = new Handler();
        }
    }

    @Test
    void shouldProperlyGetResponse() {
        when(client.execute(anyString())).thenReturn(List.of(new Result("foo")));
        assertEquals("foo", handler.request(client, "foo").get(0).get(String.class));
    }

    @Test
    void shouldProperlyGetResponseAfterFewAttempts() {
        final AtomicInteger attemptsMade = new AtomicInteger();

        when(client.execute(anyString())).thenAnswer(a -> {
            if (attemptsMade.incrementAndGet() == 2) {
                return List.of(new Result("bar"));
            }

            return List.of(new Result(Map.of("error", "bar")));
        });

        final List<Result> response = handler.tryRequest(client, "bar", 3);

        assertNotNull(response);
        assertEquals("bar", response.get(0).get(String.class));
        assertEquals(2, attemptsMade.get());
    }

    @Test
    void shouldNotGetResultAfterRanOutOfAttempts() {
        when(client.execute(anyString())).thenReturn(List.of(new Result(Map.of("error", "bar"))));

        assertNull(handler.tryRequest(client, "baz"));
    }

    @RequiredArgsConstructor
    private static class Handler extends GremlinHandler {
    }
}
