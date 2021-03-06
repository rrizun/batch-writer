package helpers;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class LocalMeter {

  private final AtomicLong sum = new AtomicLong();
  private final ConcurrentSkipListMap<Long, Long> values = new ConcurrentSkipListMap<>();
  private final int windowSeconds = 900;

  public long now() {
    return System.currentTimeMillis();
  }

  public void mark(long value) {
    sum.addAndGet(value);
    final long now = now();
    values.headMap(now - windowSeconds * 1000).clear();
    values.compute(now, (k, v) -> {
      return (v == null ? 0L : v) + value;
    });
  }

  public long sum() {
    return sum.get();
  }

  public long sum(long windowSeconds) {
    final long now = now();
    // values.headMap(now - 15 * 1000).clear();
    long fromKey = now - windowSeconds * 1000;
    long toKey = now;
    long sum = 0;
    for (long value : values.subMap(fromKey, true, toKey, false).values())
      sum += value;
    return sum;
  }

  public long avg(long windowSeconds) {
    return sum(windowSeconds) / windowSeconds;
  }

  public String toString() {
    // return String.format("%s(%s/s)", sum, avg(15));
    return String.format("%s(%s/%s/%s/%s/%s/%s)", sum, avg(1), avg(5), avg(15), avg(60), avg(300), avg(900));
  }

}
