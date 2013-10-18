package io.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.indexing.common.index.EventReceiverFirehoseFactory;
import io.druid.initialization.DruidModule;

import java.util.List;

public class IndexingServiceFirehoseModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("IndexingServiceFirehoseModule")
            .registerSubtypes(
                new NamedType(EventReceiverFirehoseFactory.class, "receiver")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
