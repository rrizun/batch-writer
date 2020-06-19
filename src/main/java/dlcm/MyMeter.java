package dlcm;

import java.util.*;
import java.util.concurrent.*;

class MyMeter {

  private long sum;
  private final NavigableMap<Long, Long> values = new ConcurrentSkipListMap<>();

  private long now() {
    long now = System.currentTimeMillis() / 1000;
    // now /= 5;
    // now *= 5;
    return now;
  }

  public void mark(long value) {
    long now = now();
    sum += value;
    values.put(now, values.getOrDefault(now, 0L) + value);
  }

  public long sum() {
    return sum;
  }

  private long sum(long periodSeconds) {
    long now = now();
    long fromKey = now - periodSeconds;
    long toKey = now;
    long sum = 0;
    for (long value : values.subMap(fromKey, true, toKey, false).values())
      sum += value;
    return sum;
  }

  public long average(long periodSeconds) {
    return sum(periodSeconds) / periodSeconds;
  }

}
