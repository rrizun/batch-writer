package helpers;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

/**
 * Opinionated robust facade/runner for listenablefuture(s).
 */
public class FutureRunner {
    
    private class VoidFuture extends AbstractFuture<Void> {
        public boolean setVoid() {
            return super.set(null);
        }
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }
      
    private int running; // aka inFlight
    private final Object lock = new Object();
    private final VoidFuture facade = new VoidFuture();
    private final List<ListenableFuture<?>> insideFutures = new CopyOnWriteArrayList<>();
    private Exception firstException;

    /**
     * ctor
     */
    public FutureRunner() {
        facade.addListener(()->{
            if (facade.isCancelled())
                insideFutures.forEach(insideFuture -> insideFuture.cancel(true));
        }, MoreExecutors.directExecutor());
    }

    public ListenableFuture<?> get() {
        synchronized (lock) {
            doFinally(); // safety
            return facade;
        }
    }

    /**
     * run
     * 
     * @param <T>
     * @param request
     */
    protected <T> void run(AsyncCallable<T> request) {
        run(request, unused -> {});
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
     * @param perRequestResponseFinally
     */
    protected <T> void run(AsyncCallable<T> request, Runnable perRequestResponseFinally) {
        run(request, unused -> {}, e->{ throw new RuntimeException(e); }, perRequestResponseFinally);
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
                        try {
                            response.accept(lf.get()); // throws
                        } catch (Exception e) {
                            try {
                                perRequestResponseCatch.accept(e); // throws
                            } catch (Exception e1) {
                                doException(e1);
                            }
                        } finally {
                            try {
                                perRequestResponseFinally.run(); // throws
                            } catch (Exception e2) {
                                doException(e2);
                            } finally {
                                doFinally();
                            }
                        }
                    }
                }, MoreExecutors.directExecutor());
            } catch (Exception e) {
                insideFutures.add(Futures.immediateFailedFuture(e));
                try {
                    try {
                        perRequestResponseCatch.accept(e); // throws
                    } catch (Exception e1) {
                        doException(e1);
                    }
                } finally {
                    try {
                        perRequestResponseFinally.run(); // throws
                    } catch (Exception e2) {
                        doException(e2);
                    } finally {
                        doFinally();
                    }
                }
            }
        }
    }

    // convenience
    protected void onFinally() {
        // do nothing
    }

    // convenience
    protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

    // ----------------------------------------------------------------------

    private void doException(Exception e) {
        if (firstException == null)
            firstException = e;
    }

    private void doFinally() {
        if (running == 0) {
            --running; // once
            try {
                onFinally();
            } catch (Exception e) {
                doException(e);
            } finally {
                if (firstException == null)
                    facade.setVoid();
                else
                    facade.setException(firstException);
            }
        }
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}
