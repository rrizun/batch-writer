package helpers;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.RateLimiter;

public class TryReservior2 {
    public static void main(String... args) throws Exception {
        new TryReservior2();
    }

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final RateLimiter limiter = RateLimiter.create(2000); // permitsPerSecond
    private int creditsPerSecond; // operational

    private final LocalMeter consumedMeter = new LocalMeter();

    private final Object lock = new Object();

    public TryReservior2() throws Exception {

        log("ctor");

        // requestMeter.mark();

        // prime
        limiter.acquire((int) limiter.getRate());

        for (int i = 0; i < 160; ++i) {
            executor.execute(() -> {
                while (true) {
                    try {

                        // log("execute!");
                        final int initial = 25;
                        // final int initial = Double.valueOf(permitsPerSecond).intValue();

                        // preAcquire
                        // log("acquire", permits);
                        acquire(initial);
                        // log("acquired", permits);

                        Thread.sleep(new Random().nextInt(500));

                        int consumed = 25;
                        // int consumed = new Random().nextInt(25);

                        consumedMeter.mark(consumed);
                        // log("requestMeter", "mean", Double.valueOf(requestMeter.getMeanRate()).longValue(), "one", Double.valueOf(requestMeter.getOneMinuteRate()).longValue());
                        log("consumedMeter", consumedMeter, limiter.getRate(), creditsPerSecond);

                        // postRelease
                        // log("release", permits-workDone);
                        release(initial - consumed);
                        // log("released", permits-workDone);

                        Thread.sleep(new Random().nextInt(2500));

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
            boolean acquired;
            do {
                int credits = Math.min(permits, creditsPerSecond);
                if (permits - credits > 0) {
                    acquired = limiter.tryAcquire(permits - credits);
                    if (!acquired) {
                        long timeout = Double.valueOf(1000 * (permits - credits) / limiter.getRate()).longValue();
                        if (timeout > 0)
                            lock.wait(timeout);
                    }
                } else
                    acquired = true;
                if (acquired)
                    creditsPerSecond -= credits;
            } while (!acquired);
            // log("acquired", permits, acquired);
        }
    }

    private void release(int credits) throws Exception {
        synchronized (lock) {
            creditsPerSecond += credits;
        }
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}