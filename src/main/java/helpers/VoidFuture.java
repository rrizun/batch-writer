package helpers;

import com.google.common.base.*;
import com.google.common.util.concurrent.*;

public class VoidFuture extends AbstractFuture<Void> {
  public boolean set(Void value) {
    return super.set(value);
  }
  public boolean set() {
    return super.set(Defaults.defaultValue(Void.class));
  }
  public boolean setVoid() {
    return super.set(Defaults.defaultValue(Void.class));
  }
  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }
}
