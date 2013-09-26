package com.englishtown.vertx;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.englishtown.vertx.hk2.MetricsBinder;

import javax.inject.Provider;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Container for read/write timers and error counters
 */
public class Metrics {

    private final Timer readTimer;
    private final Timer writeTimer;
    private final Counter readErrors;
    private final Counter writeErrors;

    public Metrics(MetricRegistry registry, String type) {

        readTimer = registry.timer(name(Metrics.class, type, "read", "success"));
        writeTimer = registry.timer(name(Metrics.class, type, "write", "success"));

        readErrors = registry.counter(name(Metrics.class, type, "read", "errors"));
        writeErrors = registry.counter(name(Metrics.class, type, "write", "errors"));
    }

    public Context timeRead() {
        return new Context(readTimer.time(), readErrors);
    }

    public Context timeWrite() {
        return new Context(writeTimer.time(), writeErrors);
    }

    public class Context {

        private final Timer.Context context;
        private final Counter errors;

        private Context(Timer.Context context, Counter errors) {
            this.context = context;
            this.errors = errors;
        }

        public void stop() {
            context.stop();
        }

        public void error() {
            stop();
            errors.inc();
        }

    }
}
