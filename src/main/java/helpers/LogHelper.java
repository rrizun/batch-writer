package helpers;

import java.util.*;

/**
 * LogHelper
 */
public class LogHelper {
  private final Object object;

  public LogHelper(Object object) {
    this.object = object;
  }

  public void log(Object... args) {
    List<String> parts = new ArrayList<>();
    // parts.add(new Date().toString());
    // parts.add(object.toString());
    for (Object arg : args)
      parts.add("" + arg);
    System.out.println(String.join(" ", parts));
  }

}