package org.apache.druid.storage.google.output;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class GoogleStorageConnectorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule(GoogleStorageConnectorModule.class.getSimpleName())
            .registerSubtypes(GoogleStorageConnectorProvider.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
