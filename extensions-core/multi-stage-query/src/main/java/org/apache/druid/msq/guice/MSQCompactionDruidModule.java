package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.indexing.common.task.CompactionToMSQ;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.compaction.CompactionToMSQImpl;
import org.apache.druid.rpc.indexing.OverlordClient;

import java.util.Collections;
import java.util.List;

public class MSQCompactionDruidModule implements DruidModule
{

  public static final String SCHEME = "msq";

  @Override
  public void configure(Binder binder)
  {
//    binder.bind(CompactionToMSQ.class).toProvider(CompactionToMSQImplProvider.class).in(LazySingleton.class);
    binder.bind(CompactionToMSQ.class).to(CompactionToMSQImpl.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Provides
  @LazySingleton
  CompactionToMSQImpl getCompactionToMSQImpl(final OverlordClient overlordClient,
                                             final ObjectMapper jsonMapper){
    return new CompactionToMSQImpl(overlordClient, jsonMapper);
  }

}
