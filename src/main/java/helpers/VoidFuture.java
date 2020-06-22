package helpers;

import com.google.common.base.Defaults;
import com.google.common.util.concurrent.AbstractFuture;

public class VoidFuture extends AbstractFuture<Void> {
  public boolean setVoid() {
    return super.set(Defaults.defaultValue(Void.class));
  }

  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }
}
