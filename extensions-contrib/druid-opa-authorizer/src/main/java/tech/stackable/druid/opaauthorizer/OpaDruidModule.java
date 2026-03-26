package tech.stackable.druid.opaauthorizer;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import java.util.List;
import org.apache.druid.initialization.DruidModule;

public class OpaDruidModule implements DruidModule {

  @Override
  public void configure(Binder binder) {}

  @Override
  public List<? extends Module> getJacksonModules() {
    return ImmutableList.of(new SimpleModule("Opa").registerSubtypes(OpaAuthorizer.class));
  }
}
