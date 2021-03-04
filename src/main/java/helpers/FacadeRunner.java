package helpers;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

/**
 * Opinionated robust facade/runner for listenablefuture(s).
 */
public class FacadeRunner {
    
    private int running;
    private final Object lock = new Object();
    private final VoidFuture voidFuture = new VoidFuture();
    private final List<ListenableFuture<?>> insideFutures = new CopyOnWriteArrayList<>();

    /**
     * ctor
     */
    public FacadeRunner() {
        voidFuture.addListener(()->{
            if (voidFuture.isCancelled())
                insideFutures.forEach(lf -> lf.cancel(true));
        }, MoreExecutors.directExecutor());
    }

    public ListenableFuture<?> all() {
        return voidFuture;
    }

    public ListenableFuture<?> one() {
        return insideFutures.iterator().next();
    }

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
        run(request, response, e -> {
            log("FutureRunner.catch[1]", Throwables.getRootCause(e));
        });
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     * @param perRequestResponseFinally
     */
    protected <T> void run(AsyncCallable<T> request, Runnable perRequestResponseFinally) {
        run(request, response -> {}, e->{
            log("FutureRunner.catch[1]", Throwables.getRootCause(e));
        }, perRequestResponseFinally);
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
                insideFutures.add(lf);
                lf.addListener(() -> {
                    synchronized (lock) {
                        --running;
                        // insideFutures.remove(lf);
                        try {
                            response.accept(lf.get()); // throws
                        } catch (Exception e) {
                            try {
                                perRequestResponseCatch.accept(e); // throws
                            } catch (Exception e3a) {
                                log("FutureRunner.catch[3a]", Throwables.getRootCause(e3a));
                            }
                        } finally {
                            try {
                                perRequestResponseFinally.run(); // throws
                            } catch (Exception e3b) {
                                log("FutureRunner.catch[3b]", Throwables.getRootCause(e3b));
                            } finally {
                                if (running == 0)
                                    voidFuture.setVoid();
                            }
                        }
                    }
                }, MoreExecutors.directExecutor());
            } catch (Exception e) {
                insideFutures.add(Futures.immediateFailedFuture(e));
                try {
                    try {
                        perRequestResponseCatch.accept(e); // throws
                    } catch (Exception e2a) {
                        log("FutureRunner.catch[2a]", Throwables.getRootCause(e2a));
                    }
                } finally {
                    try {
                        perRequestResponseFinally.run(); // throws
                    } catch (Exception e2b) {
                        log("FutureRunner.catch[2b]", Throwables.getRootCause(e2b));
                    } finally {
                        if (running == 0)
                            voidFuture.setVoid();
                    }
                }
            }
        }
    }

    // convenience
    protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}
