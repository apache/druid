package com.metamx.druid.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;


/**
 */
public class LogLevelAdjuster implements LogLevelAdjusterMBean
{
  private static final Logger log = Logger.getLogger(LogLevelAdjuster.class);

  private static volatile boolean registered = false;
  public synchronized static void register() throws Exception
  {
    if (! registered) {
      ManagementFactory.getPlatformMBeanServer().registerMBean(
          new LogLevelAdjuster(),
          new ObjectName("log4j:name=log4j")
      );
      registered = true;
    }
  }

  @Override
  public String getLevel(String packageName)
  {
    final Level level = Logger.getLogger(packageName).getEffectiveLevel();

    if (log.isInfoEnabled()) {
      log.info(String.format("Asked to look up level for package[%s] => [%s]", packageName, level));
    }

    return level.toString();
  }

  @Override
  public void setLevel(String packageName, String level)
  {
    final Level theLevel = Level.toLevel(level, null);
    if (theLevel == null) {
      throw new IllegalArgumentException("Unknown level: " + level);
    }

    if (log.isInfoEnabled()) {
      log.info(String.format("Setting log level for package[%s] => [%s]", packageName, theLevel));
    }

    Logger.getLogger(packageName).setLevel(theLevel);
  }
}
