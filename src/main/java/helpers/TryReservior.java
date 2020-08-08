package helpers;

import java.time.Duration;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

public class TryReservior {
    public static void main(String... args) {
        new TryReservior();
    }

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final RateLimiter limiter = RateLimiter.create(128);

    private final MetricRegistry registry = new MetricRegistry();
    private final Meter requestMeter = registry.meter("requestMeter");
    private final MyMeter myMeter = new MyMeter();

    // private final SlidingTimeWindowReservoir slidingTimeWindowReservoir = new
    // SlidingTimeWindowReservoir(1, TimeUnit.SECONDS);

    private final Object lock = new Object();
    private final ConcurrentSkipListMap<Long, Long> credits = new ConcurrentSkipListMap<>();

    public TryReservior() {

        log("ctor");

        // requestMeter.mark();
        limiter.acquire(Double.valueOf(limiter.getRate()).intValue());

        // while (true)
        {
            for (int i = 0; i < 160; ++i) {
                executor.execute(() -> {
                    while (true) {
                        // log("execute!");
                        final int permits = 128;

                            // preAcquire
                            // log("acquire", permits);
                            acquire(permits);
                            // log("acquired", permits);

                            final int workDone = 1;
                            // final int workDone = new Random().nextInt(128);
                            // log("workDone", workDone);
                            requestMeter.mark(workDone);
                            myMeter.mark(workDone);
                            // slidingTimeWindowReservoir.update(workDone);

                            // postRelease
                            // log("release", permits-workDone);
                            release(permits - workDone);
                            // log("released", permits-workDone);

                        log("requestMeter", "mean", Double.valueOf(requestMeter.getMeanRate()).longValue(), "one", Double.valueOf(requestMeter.getOneMinuteRate()).longValue());
                        log("##########slidingTimeWindowReservoir", myMeter);
                    }
                });
            }
        }

        // MoreExecutors.shutdownAndAwaitTermination(executor,
        // Duration.ofMillis(Long.MAX_VALUE));

    }

    private final long window = 1000;

    private void acquire(int permits) {
        synchronized (lock) {
            final long now = System.currentTimeMillis();
            credits.headMap(now - window).clear();
            
            for (long key : credits.keySet()) {
                long value = credits.get(key);
    
                long credit = Math.min(value, permits);
                value -= credit;
                permits -= credit;
    
                credits.put(key, value);
            }
        }

        if (permits > 0)
            limiter.acquire(permits);

        // log("acquired", permits);
    }
    private void release(int permits) {
        synchronized (lock) {
            final long now = System.currentTimeMillis();
            credits.headMap(now - window).clear();

            credits.put(now, credits.getOrDefault(now, 0L) + permits);
            // log("released", permits);
        }
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}