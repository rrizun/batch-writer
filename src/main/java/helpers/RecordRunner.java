package helpers;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.micrometer.core.instrument.Metrics;

public class RecordRunner<R> {
    public boolean success;
    public String failureMessage;
    public final R record;
    public static <R> RecordRunner<R> record(R record) {
        return new RecordRunner<R>(record);
    }
    private RecordRunner(R record) {
        this.record = record;
    }
    public RecordRunner<R> metric(String metricName) {
        return this;
    }
    public <V> ListenableFuture<V> run(AsyncFunction<R, V> async) {
        try {
            ListenableFuture<V> lf = async.apply(record);
            lf.addListener(() -> {
                try {
                    lf.get();
                    success = true;
                    Metrics.counter(record.getClass().getName(), "success", "true").increment();
                } catch (Exception e) {
                    failureMessage = ""+e;
                    Metrics.counter(record.getClass().getName(), "success", "false").increment();
                } finally {
                    log(SplunkHelper.render(this));
                }
            }, MoreExecutors.directExecutor());
            return lf;
        } catch (Exception e) {
            failureMessage = ""+e;
            Metrics.counter(record.getClass().getName(), "success", "false").increment();
            try {
                return Futures.immediateFailedFuture(e);
            } finally {
                log(SplunkHelper.render(this));
            }
        }
    }
    private void log(Object... args) {
        new LogHelper(record).log(args);
    }
}
