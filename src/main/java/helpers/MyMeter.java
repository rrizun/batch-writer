package helpers;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class MyMeter {

  private final NavigableMap<Long, AtomicLong> values = new ConcurrentSkipListMap<>();

  private long now() {
    long now = System.currentTimeMillis();
    // now /= 5;
    // now *= 5;
    return now;
  }

  public void mark(long value) {
    long now = now();
    values.computeIfAbsent(now, k -> new AtomicLong()).addAndGet(value);
    // keep 15min worth
    // values.keySet().retainAll(values.tailMap(now - 900*1000).keySet());
  }

  public long sum() {
    long sum = 0;
    for (AtomicLong value : values.values())
      sum += value.get();
    return sum;
  }

  private long sum(long periodSeconds) {
    long now = now();
    long fromKey = now - periodSeconds*1000;
    long toKey = now;
    long sum = 0;
    for (AtomicLong value : values.subMap(fromKey, true, toKey, false).values())
      sum += value.get();
    return sum;
  }

  public long average(long periodSeconds) {
    return sum(periodSeconds) / periodSeconds;
  }

  public String toString() {
    return String.format("%s/%s", average(5), sum());
    // return String.format("%s[%s/%s/%s]%s", average(5), average(60), average(300), average(900), sum());
  }

}
