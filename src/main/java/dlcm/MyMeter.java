package dlcm;

import java.util.*;

class MyMeter {

  private final long periodSeconds;
  private final NavigableMap<Long, Long> values = new TreeMap<>();

  public MyMeter(long periodSeconds) {
    this.periodSeconds = periodSeconds;
  }

  public void mark(long value) {
    synchronized(this) {
      long now = System.currentTimeMillis()/1000;
      values.put(now, values.getOrDefault(now, 0L)+value);
      }
  }

  public long sum() {
    synchronized(this) {
      long now = System.currentTimeMillis()/1000;
      long fromKey = now - periodSeconds;
      long toKey = now;
      long sum = 0;
      for (long value : values.subMap(fromKey, true, toKey, false).values())
        sum += value;
      return sum;
    }
  }

  public long average() {
    return sum()/periodSeconds;
  }
}
