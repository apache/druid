package org.apache.druid.server.metrics;

public interface WorkerTaskCountStatsProvider
{
  /**
   * Return the number of failed tasks run on middle-manager during emission period.
   */
  Long getFailedTaskCount();

  /**
   * Return the number of current running tasks on middle-manager.
   */
  Long getRunningTaskCount();

  /**
   * Return the number of successful tasks run on middle-manager during emission period.
   */
  Long getSuccessfulTaskCount();
}
