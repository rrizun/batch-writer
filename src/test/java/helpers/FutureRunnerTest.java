package helpers;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
    
    assertThrows(Exception.class, ()->{
      new FutureRunner() {
        {
          run(() -> {
            throw new Exception("Oof!");
          });
        }
      }.get().get();
    });

    assertThrows(Exception.class, ()->{
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("Oof!"));
          });
        }
      }.get().get();
    });
    
  }

  @Test
  public void adversarial() {
    Exception e;

    e = assertThrows(Exception.class, ()->{
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("[1]"));
          });
        }
      }.get().get();
    });
    log(e);
    assertEquals("[1]", Throwables.getRootCause(e).getMessage());

    e = assertThrows(Exception.class, ()->{
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("[1]"));
          });
        }
      }.get().get();
    });
    log(e);
    assertEquals("[1]", Throwables.getRootCause(e).getMessage());
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

  public static void main(String... args) throws Exception {
    try {
      ListenableFuture<?> lf = new FutureRunner(){{
        run(()->{
          throw new Exception("Oof!");
          // return Futures.immediateVoidFuture();
        }, result->{
          System.out.println(" [result] "+result);
        }, e->{
          System.out.println(" [handled] "+e);
          throw new RuntimeException(e);
        });
      }}.get();
      System.out.println(" [1] "+lf);
      System.out.println(" [2] "+lf.get());
    } catch (Exception e) {
      System.out.println(" [3] "+e);
    } finally {
      System.out.println("done");
    }
  }

}
