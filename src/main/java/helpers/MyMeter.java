package helpers;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MyMeter {

  private final NavigableMap<Long, AtomicLong> values = new ConcurrentSkipListMap<>();

  private long now() {
    long now = System.currentTimeMillis();
    // now /= 5;
    // now *= 5;
    return now;
  }

  public void mark(long value) {
    long now = now();
    values.headMap(now - 900*1000).clear();
    values.computeIfAbsent(now, k -> new AtomicLong()).addAndGet(value);
  }

  public long sum() {
    values.headMap(now() - 900*1000).clear();
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
    return String.format("%s(%s/s)", sum(), average(15));
    // return String.format("%s(%s/%s/%s)", sum(), average(1), average(5), average(15));
  }

}
