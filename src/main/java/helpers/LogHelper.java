package helpers;

import java.util.*;

import org.slf4j.*;

/**
 * LogHelper
 */
public class LogHelper {
  private final Logger logger;

  public LogHelper(Object classOrInstance) {
    Class<?> classOfT = classOrInstance.getClass();
    if (classOrInstance instanceof Class)
      classOfT = (Class<?>) classOrInstance;
    logger = LoggerFactory.getLogger(classOfT);
  }

  public void log(Object... args) {
    List<String> parts = new ArrayList<>();
    for (Object arg : args)
      parts.add("" + arg);
    logger.info(String.join(" ", parts));
  }

  // static {
  // ((ch.qos.logback.classic.Logger)
  // org.slf4j.LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.INFO);
  // }

}