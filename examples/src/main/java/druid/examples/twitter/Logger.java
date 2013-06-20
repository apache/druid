package druid.examples.twitter;

/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;



import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.0
 */
public abstract class Logger {
  private static final LoggerFactory LOGGER_FACTORY;
  private static final String LOGGER_FACTORY_IMPLEMENTATION = "twitter4j.loggerFactory";

  static {
    LoggerFactory loggerFactory = null;
    // -Dtwitter4j.debug=true -Dtwitter4j.loggerFactory=twitter4j.internal.logging.StdOutLoggerFactory
    String loggerFactoryImpl = System.getProperty(LOGGER_FACTORY_IMPLEMENTATION);
    if (loggerFactoryImpl != null) {
      loggerFactory = getLoggerFactoryIfAvailable(loggerFactoryImpl, loggerFactoryImpl);
    }

    Configuration conf = ConfigurationContext.getInstance();
    // configuration in twitter4j.properties
    // loggerFactory=twitter4j.internal.logging.StdOutLoggerFactory
    loggerFactoryImpl = conf.getLoggerFactory();
    if (loggerFactoryImpl != null) {
      loggerFactory = getLoggerFactoryIfAvailable(loggerFactoryImpl, loggerFactoryImpl);
    }
    // use SLF4J if it's found in the classpath
    if (null == loggerFactory) {
      loggerFactory = getLoggerFactoryIfAvailable("org.slf4j.impl.StaticLoggerBinder", "twitter4j.internal.logging.SLF4JLoggerFactory");
    }
    // otherwise, use commons-logging if it's found in the classpath
    if (null == loggerFactory) {
      loggerFactory = getLoggerFactoryIfAvailable("org.apache.commons.logging.Log", "twitter4j.internal.logging.CommonsLoggingLoggerFactory");
    }
    // otherwise, use log4j if it's found in the classpath
    if (null == loggerFactory) {
      loggerFactory = getLoggerFactoryIfAvailable("org.apache.log4j.Logger", "twitter4j.internal.logging.Log4JLoggerFactory");
    }
    // on Google App Engine, use java.util.logging
    if (null == loggerFactory) {
      loggerFactory = getLoggerFactoryIfAvailable("com.google.appengine.api.urlfetch.URLFetchService", "twitter4j.internal.logging.JULLoggerFactory");
    }
    // otherwise, use the default logger
//    if (null == loggerFactory) {
//      loggerFactory = new StdOutLoggerFactory();
//    }
    LOGGER_FACTORY = loggerFactory;

    try {
      Method method = conf.getClass().getMethod("dumpConfiguration", new Class[]{});
      method.setAccessible(true);
      method.invoke(conf);
    } catch (IllegalAccessException ignore) {
    } catch (InvocationTargetException ignore) {
    } catch (NoSuchMethodException ignore) {
    }
  }

  private static LoggerFactory getLoggerFactoryIfAvailable(String checkClassName, String implementationClass) {
    try {
      Class.forName(checkClassName);
      return (LoggerFactory) Class.forName(implementationClass).newInstance();
    } catch (ClassNotFoundException ignore) {
    } catch (InstantiationException e) {
      throw new AssertionError(e);
    } catch (SecurityException ignore) {
      // Unsigned applets are not allowed to access System properties
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
    return null;
  }

  /**
   * Returns a Logger instance associated with the specified class.
   *
   * @param clazz class
   * @return logger instance
   */
  public static Logger getLogger(Class clazz) {
    return LOGGER_FACTORY.getLogger(clazz);
  }

  /**
   * tests if debug level logging is enabled
   *
   * @return if debug level logging is enabled
   */
  public abstract boolean isDebugEnabled();

  /**
   * tests if info level logging is enabled
   *
   * @return if info level logging is enabled
   */
  public abstract boolean isInfoEnabled();

  /**
   * tests if warn level logging is enabled
   *
   * @return if warn level logging is enabled
   */
  public abstract boolean isWarnEnabled();

  /**
   * tests if error level logging is enabled
   *
   * @return if error level logging is enabled
   */
  public abstract boolean isErrorEnabled();

  /**
   * @param message message
   */
  public abstract void debug(String message);

  /**
   * @param message  message
   * @param message2 message2
   */
  public abstract void debug(String message, String message2);

  /**
   * @param message message
   */
  public abstract void info(String message);

  /**
   * @param message  message
   * @param message2 message2
   */
  public abstract void info(String message, String message2);

  /**
   * @param message message
   */
  public abstract void warn(String message);

  /**
   * @param message  message
   * @param message2 message2
   */
  public abstract void warn(String message, String message2);

  /**
   * @param message message
   */
  public abstract void error(String message);

  /**
   * @param message message
   * @param th      throwable
   */
  public abstract void error(String message, Throwable th);

}
