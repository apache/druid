package com.metamx.druid.realtime;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;

/**
 */
public interface Plumber
{
  public Sink getSink(long timestamp);
  public <T> QueryRunner<T> getQueryRunner(Query<T> query);
  void persist(Runnable commitRunnable);
  public void finishJob();
}
