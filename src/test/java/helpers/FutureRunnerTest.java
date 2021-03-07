package helpers;

import static org.assertj.core.api.Assertions.*;
// import static org.junit.jupiter.api.Assertions.*;

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

    new FutureRunner();

    new FutureRunner(){};

    new FutureRunner(){{}};

    new FutureRunner(){{}}.get();

    new FutureRunner(){{}}.get().get();
    
    // future runner is a facade for zero or more internal futures
    assertThat(new FutureRunner() {
      {
        run(() -> {
          return Futures.immediateFuture("hello");
        });
      }
    }.get().get()).isNull();

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            throw new Exception("Oof[1]");
          });
        }
      }.get().get();
    }).hasMessageContaining("Oof[1]");

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("Oof[2]"));
          });
        }
      }.get().get();
    }).hasMessageContaining("Oof[2]");

    // if more that one thrown exception then return the first thrown exception
    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("first"));
          });
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("second"));
          });
        }
      }.get().get();
    }).hasMessageContaining("first").hasMessageNotContaining("second");

  }

  /**
   * adversarial
   * 
   * @throws Exception
   */
  @Test
  public void adversarial() throws Exception {

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            throw new Exception("fromRun"); // first
          });
        }
      }.get().get();
    }).hasMessageContaining("fromRun");

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFailedFuture(new Exception("fromRun")); // first
          });
        }
      }.get().get();
    }).hasMessageContaining("fromRun");

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateFuture("hello");
          }, result -> {
            throw new RuntimeException("fromResult"); // first
          });
        }
      }.get().get();
    }).hasMessageContaining("fromResult");

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateVoidFuture();
          }, result -> {
            throw new RuntimeException("fromResult");
          }, e -> {
            throw new RuntimeException("fromCatch", e); // first
          });
        }
      }.get().get();
    }).hasMessageContaining("fromCatch");

    assertThatThrownBy(() -> {
      new FutureRunner() {
        {
          run(() -> {
            return Futures.immediateVoidFuture();
          }, result -> {
            throw new RuntimeException("fromResult");
          }, e -> {
            throw new RuntimeException("fromCatch", e); // first
          }, ()->{
            throw new RuntimeException("fromFinally"); // second
          });
        }
      }.get().get();
    }).hasMessageContaining("fromCatch");

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
