/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.confluent.druid.transform.ExtractTenantTopicTransform;
import io.confluent.druid.transform.ExtractTenantTransform;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class ConfluentExtensionsModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ConfluentTransformsModule")
            .registerSubtypes(
                new NamedType(ExtractTenantTransform.class, "extractTenant"),
                new NamedType(ExtractTenantTopicTransform.class, "extractTenantTopic")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
