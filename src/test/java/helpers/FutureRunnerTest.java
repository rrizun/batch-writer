package helpers;

import com.google.common.util.concurrent.Futures;

import org.junit.jupiter.api.Test;

/**
 * FutureRunnerTest
 */
public class FutureRunnerTest {

  /**
   * basicSmoke
   * 
   * @throws Exception
   */
  @Test
  public void basicSmoke() throws Exception {
    new FutureRunner() {
      {
        run(() -> {
          return Futures.immediateVoidFuture();
        });
      }
    }.get().get();
    new FutureRunner() {
      {
        run(() -> {
          return Futures.immediateVoidFuture();
        });
      }
    }.one().get();
  }

  /**
   * test1a
   * 
   * @throws Exception
   */
  @Test
  public void test1a() throws Exception {

    new FutureRunner() {
      {
        log("init");
        run(() -> {
          throw new Exception("fromRun");
        });
      }
    }.get().get();

    new FutureRunner() {
      {
        log("init");
        run(() -> {
          return Futures.immediateVoidFuture();
        }, result -> {
          throw new RuntimeException("fromResult");
        });
      }
    }.get().get();

    new FutureRunner() {
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
    }.get().get();

    new FutureRunner() {
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
    }.get().get();

  }

  /**
   * test1b
   * 
   * @throws Exception
   */
  @Test
  public void test1b() throws Exception {

    new FutureRunner() {
      {
        log("init");
        run(() -> {
          return Futures.immediateFailedFuture(new Exception("fromRun"));
        });
      }
    }.get().get();

    new FutureRunner() {
      {
        log("init");
        run(() -> {
          return Futures.immediateFailedFuture(new Exception("fromRun"));
        }, result -> {
        }, e -> {
          throw new RuntimeException("fromCatch");
        });
      }
    }.get().get();

    new FutureRunner() {
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
    }.get().get();

  }

  class MyAuditRecord {
    public boolean success;
    public String failureMessage;
    public Number result;
    public String toString() {
      return SplunkHelper.toString(this);
    }
  }

  @Test
  public void testAuditRecord() throws Exception {
    new FutureRunner() {
      {
        MyAuditRecord record = new MyAuditRecord();
        run(() -> {
          return Futures.immediateFuture(1.0 / 0);
          // return Futures.immediateFuture(new Supplier<Number>(){
          // @Override
          // public Number get() {
          // return 1.0/0;
          // }
          // });
        }, result -> {
          record.result = result;
          record.success = true;
        }, e -> {
          record.failureMessage = e.toString();
        }, () -> {
          log(record);
        });
      }
    }.get().get();
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
