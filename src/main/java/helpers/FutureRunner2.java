package helpers;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

/**
 * Opinionated robust facade/runner for listenablefuture(s).
 */
public class FutureRunner2 extends AbstractFuture<Void> {

    private int running;
    private final Object lock = new Object();
    private final Collection<ListenableFuture<?>> futures = new CopyOnWriteArrayList<>();

    /**
     * run
     * 
     * @param <T>
     * @param request
     */
    protected <T> void run(AsyncCallable<T> request) {
        run(request, response -> {});
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     * @param response
     */
    protected <T> void run(AsyncCallable<T> request, Consumer<T> response) {
        run(request, response, e -> { throw new RuntimeException(e); });
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     * @param response
     * @param perRequestResponseCatch
     */
    protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch) {
        run(request, response, perRequestResponseCatch, () -> {});
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     * @param response
     * @param perRequestResponseCatch
     * @param perRequestResponseFinally
     */
    protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch, Runnable perRequestResponseFinally) {
        synchronized (lock) {
            try {
                ListenableFuture<T> lf = request.call(); // throws
                ++running;
                futures.add(lf);
                lf.addListener(() -> {
                    synchronized (lock) {
                        --running;
                        futures.remove(lf);
                        try {
                            response.accept(lf.get()); // throws
                        } catch (Exception e1) {
                            try {
                                perRequestResponseCatch.accept(e1); // throws
                            } catch (Exception e2) {
                                e2.printStackTrace();
                            }
                        } finally {
                            try {
                                perRequestResponseFinally.run();
                            } finally {
                                if (running == 0)
                                    set(Defaults.defaultValue(Void.class)); // set futurerunner result
                            }
                        }
                    }
                }, MoreExecutors.directExecutor());
            } catch (Exception e1) {
                try {
                    try {
                        perRequestResponseCatch.accept(e1); // throws
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                } finally {
                    try {
                        perRequestResponseFinally.run();
                    } finally {
                        if (running == 0)
                            set(Defaults.defaultValue(Void.class)); // set futurerunner result
                    }
                }
            }
        }
    }

    @Override
    protected void afterDone() {
        super.afterDone();
        if (isCancelled())
            futures.forEach(lf -> lf.cancel(true));
    }

    // convenience
    protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

}
