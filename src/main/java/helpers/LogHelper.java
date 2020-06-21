package helpers;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogHelper
 */
public class LogHelper {
  private final Logger logger;

  public LogHelper(Object object) {
    logger = LoggerFactory.getLogger(object.getClass());
  }

  public void log(Object... args) {
    logger.info(str(args));
  }

  public void debug(Object... args) {
    logger.debug(str(args));
  }

  public void trace(Object... args) {
    logger.trace(str(args));
  }

  public String str(Object... args) {
    List<String> parts = new ArrayList<>();
    for (Object arg : args)
      parts.add("" + arg);
    return String.join(" ", parts);
  }

  // static {
  //   ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.INFO);
  // }

}