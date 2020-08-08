package helpers;

import java.time.Duration;
import java.util.Queue;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

public class TryReservior2 {
    public static void main(String... args) throws Exception {
        new TryReservior2();
    }

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final double permitsPerSecond = 10000;
    private final RateLimiter limiter = RateLimiter.create(permitsPerSecond);

    private final MetricRegistry registry = new MetricRegistry();
    private final Meter requestMeter = registry.meter("requestMeter");

    private final MyMeter myMeter = new MyMeter();

    private final Object lock = new Object();

    public TryReservior2() throws Exception {

        log("ctor");

        // requestMeter.mark();
        // limiter.acquire(Double.valueOf(limiter.getRate()).intValue());

        for (int i = 0; i < 400; ++i) {
            executor.execute(() -> {
                while (true) {
                    try {

                        // log("execute!");
                        final int initial = 128;

                        // preAcquire
                        // log("acquire", permits);
                        acquire(initial);
                        // log("acquired", permits);

                        int consumed = initial;
                        // int consumed = new Random().nextInt(128);

                        requestMeter.mark(consumed);
                        myMeter.mark(consumed);
                        // slidingTimeWindowReservoir.update(workDone);

                        // postRelease
                        // log("release", permits-workDone);
                        release(initial - consumed);
                        // log("released", permits-workDone);

                        // log("requestMeter", "mean", Double.valueOf(requestMeter.getMeanRate()).longValue(), "one", Double.valueOf(requestMeter.getOneMinuteRate()).longValue());
                        log("meter", myMeter, "credits", credits);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        // MoreExecutors.shutdownAndAwaitTermination(executor,
        // Duration.ofMillis(Long.MAX_VALUE));

    }

    private int credits;

    private void acquire(int permits) throws Exception {
        synchronized (lock) {
            try {
                while (!tryAcquire(permits)) {
                    long timeout = Double.valueOf(1000*permits/permitsPerSecond).longValue();
                    if (timeout>0)
                        lock.wait(timeout);
                    // log("waited", timeout);
                    // lock.wait(Math.max(timeout, 1));
                }
            } finally {
                lock.notifyAll();
            }
        }
    }

    private boolean tryAcquire(int permits) {
        int value = Math.min(permits, credits);
        permits -= value;
        if (permits > 0) {
            // long timeout = Double.valueOf(1000*permits/permitsPerSecond).longValue();
            if (!limiter.tryAcquire(permits))
                return false;
        }
        credits -= value;
        return true;
    }

    private void release(int permits) throws Exception {
        synchronized (lock) {
            try {
                credits += permits;
            } finally {
                lock.notifyAll();
            }
        }
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}