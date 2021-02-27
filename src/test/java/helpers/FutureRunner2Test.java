package helpers;

import com.google.common.util.concurrent.Futures;

import org.junit.jupiter.api.Test;

/**
 * FutureRunner2Test
 */
public class FutureRunner2Test {

  /**
   * basicSmoke
   * 
   * @throws Exception
   */
  @Test
  public void basicSmoke() throws Exception {
    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateVoidFuture();
        });
      }
    }.get();
  }

  /**
   * test1a
   * 
   * @throws Exception
   */
  @Test
  public void test1a() throws Exception {

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          throw new Exception("fromRun");
        });
      }
    }.get();

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateVoidFuture();
        }, result -> {
          throw new RuntimeException("fromResult");
        });
      }
    }.get();

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateVoidFuture();
        }, result -> {
          throw new RuntimeException("fromResult");
        }, e -> {
          throw new RuntimeException("fromCatch");
        });
      }
    }.get();

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateVoidFuture();
        }, result -> {
          throw new RuntimeException("fromResult");
        }, e -> {
          throw new RuntimeException("fromCatch");
        }, ()->{
          throw new RuntimeException("fromFinally");
        });
      }
    }.get();

  }

  /**
   * test1b
   * 
   * @throws Exception
   */
  @Test
  public void test1b() throws Exception {

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateFailedFuture(new Exception("fromRun"));
        });
      }
    }.get();

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateFailedFuture(new Exception("fromRun"));
        }, result -> {
        }, e -> {
          throw new RuntimeException("fromCatch");
        });
      }
    }.get();

    new FutureRunner2() {
      {
        log("init");
        run(() -> {
          return Futures.immediateFailedFuture(new Exception("fromRun"));
        }, result -> {
        }, e -> {
          throw new RuntimeException("fromCatch");
        }, ()->{
          throw new RuntimeException("fromFinally");
        });
      }
    }.get();

  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
