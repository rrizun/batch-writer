package helpers;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;

public class TryReservior2 {
    public static void main(String... args) throws Exception {
        new TryReservior2();
    }

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // config
    private final double permitsPerSecond = 2000.0;
    
    // operational
    private int creditsPerSecond;

    private final RateLimiter limiter = RateLimiter.create(permitsPerSecond);
    private final LocalMeter consumedMeter = new LocalMeter();

    private final Object lock = new Object();

    public TryReservior2() throws Exception {

        log("ctor");

        // requestMeter.mark();

        // prime
        limiter.acquire(Double.valueOf(limiter.getRate()).intValue());

        for (int i = 0; i < 1; ++i) {
            executor.execute(() -> {
                while (true) {
                    try {

                        // log("execute!");
                        final int initial = 128;

                        // preAcquire
                        // log("acquire", permits);
                        acquire(initial);
                        // log("acquired", permits);

                        // int consumed = 128;
                        int consumed = new Random().nextInt(initial);

                        consumedMeter.mark(consumed);
                        // slidingTimeWindowReservoir.update(workDone);

                        // postRelease
                        // log("release", permits-workDone);
                        release(initial - consumed);
                        // log("released", permits-workDone);

                        // log("requestMeter", "mean", Double.valueOf(requestMeter.getMeanRate()).longValue(), "one", Double.valueOf(requestMeter.getOneMinuteRate()).longValue());
                        log("consumedMeter", consumedMeter);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        // MoreExecutors.shutdownAndAwaitTermination(executor,
        // Duration.ofMillis(Long.MAX_VALUE));

    }

    private void acquire(int permits) throws Exception {
        synchronized (lock) {
            try {
                boolean acquired = true;
                do {
                    int credits = Math.min(permits, creditsPerSecond);
                    if (permits - credits > 0) {
                        acquired = limiter.tryAcquire(permits - credits);
                        if (!acquired) {
                            long timeout = Double.valueOf(1000*permits/permitsPerSecond).longValue();
                            if (timeout > 0)
                                lock.wait(timeout);
                        }
                    }
                    if (acquired)
                        creditsPerSecond -= credits;
                } while (!acquired);
            } finally {
                lock.notifyAll();
            }
        }
    }

    private void release(int credits) throws Exception {
        synchronized (lock) {
            try {
                creditsPerSecond += credits;
            } finally {
                lock.notifyAll();
            }
        }
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}