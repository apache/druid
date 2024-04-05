package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.indexing.common.task.CompactionToMSQ;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.compaction.CompactionToMSQImplProvider;

import java.util.Collections;
import java.util.List;

public class MSQCompactionDruidModule implements DruidModule
{

  public static final String SCHEME = "msq";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(CompactionToMSQ.class).toProvider(CompactionToMSQImplProvider.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

}
