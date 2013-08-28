package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.log.EmittingRequestLoggerProvider;
import com.metamx.druid.http.log.FileRequestLoggerProvider;
import com.metamx.druid.http.log.RequestLogger;
import com.metamx.druid.http.log.RequestLoggerProvider;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

/**
 */
public class QueryableModule implements DruidModule
{
  private final Class<? extends QuerySegmentWalker> walkerClass;

  public QueryableModule(
      Class<? extends QuerySegmentWalker> walkerClass
  )
  {
    this.walkerClass = walkerClass;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(QueryServlet.class).in(LazySingleton.class);
    binder.bind(QuerySegmentWalker.class).to(walkerClass).in(LazySingleton.class);
    binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.request.logging", RequestLoggerProvider.class);
  }

  @Override
  public List<Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("QueryableModule")
            .registerSubtypes(EmittingRequestLoggerProvider.class, FileRequestLoggerProvider.class)
    );
  }
}
