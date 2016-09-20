package io.druid.hadoopMultiIndexTaskExt;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 *
 */
public class HadoopMultiIndexTaskModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("HadoopMultiIndexTaskModule")
            .registerSubtypes(
                new NamedType(HadoopMultiIndexTask.class, "multi_index_hadoop")
            )
    );
  }

  @Override
  public void configure(Binder binder) {}
}
