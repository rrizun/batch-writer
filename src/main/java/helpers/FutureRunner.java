package helpers;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.base.Defaults;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

/**
 * Opinionated robust facade/runner for one or more listenablefuture(s).
 */
public class FutureRunner extends AbstractFuture<Void> {

    private int running;
    private final Object lock = new Object();
    private final Collection<ListenableFuture<?>> futures = Sets.newConcurrentHashSet();

    // ----------------------------------------------------------------------

    public FutureRunner() {
        addListener(() -> {
            if (isCancelled()) {
                for (ListenableFuture<?> f : futures)
                    f.cancel(true);
            }
        }, MoreExecutors.directExecutor());
    }

    protected <T> void run(AsyncCallable<T> request) {
        run(request, response -> {
        }, e -> {
        }, () -> {
        });
    }

    protected <T> void run(AsyncCallable<T> request, Consumer<T> response) {
        run(request, response, e -> {
        }, () -> {
        });
    }

    protected <T> void run(AsyncCallable<T> request, Runnable perRequestResponseFinally) {
        run(request, response -> {
        }, e -> {
        }, perRequestResponseFinally);
    }

    protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Runnable perRequestResponseFinally) {
        run(request, response, e -> {
        }, perRequestResponseFinally);
    }

    protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch) {
        run(request, response, perRequestResponseCatch, () -> {
        });
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     * @param response
     * @param perRequestResponseCatch   per-request/response catch
     * @param perRequestResponseFinally per-request/response finally
     */
    protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch, Runnable perRequestResponseFinally) {
        synchronized (lock) {
            try {
                ++running;
                ListenableFuture<T> f = request.call(); // throws
                futures.add(f);
                f.addListener(() -> {
                    synchronized (lock) {
                        --running;
                        try {
                            response.accept(f.get()); // throws
                        } catch (Exception e1) {
                            try {
                                perRequestResponseCatch.accept(e1); // throws
                            } catch (Exception e2) {
                                setException(e2); // fail fast
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
                --running;
                try {
                    try {
                        perRequestResponseCatch.accept(e1); // throws
                    } catch (Exception e2) {
                        setException(e2); // fail fast
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

    // ----------------------------------------------------------------------

    // convenience
    protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

            // @Override
            // protected void afterDone() {
            //     super.afterDone();
            //     // ###TODO use addListener here instead??
            //     // ###TODO use addListener here instead??
            //     // ###TODO use addListener here instead??
            //     // ###TODO use addListener here instead??
            //     // ###TODO use addListener here instead??
            //     synchronized (lock) {//###TODOlock not needed??
            //         if (isCancelled()) {
            //             for (ListenableFuture<?> f : futures)
            //                 f.cancel(true);
            //         }
            //     }
            // }

}
