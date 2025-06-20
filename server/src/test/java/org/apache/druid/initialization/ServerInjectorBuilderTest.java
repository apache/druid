/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.initialization;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ExtensionsLoader;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.TestDruidModule;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

public class ServerInjectorBuilderTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Injector startupInjector()
  {
    return new StartupInjectorBuilder()
        .withEmptyProperties()
        .withExtensions()
        .build();
  }

  @Test
  public void test03ClassLoaderExtensionsLoading()
  {
    Injector startupInjector = startupInjector();
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);

    Function<DruidModule, String> fnClassName = new Function<>()
    {
      @Nullable
      @Override
      public String apply(@Nullable DruidModule input)
      {
        return input.getClass().getName();
      }
    };

    Collection<DruidModule> modules = extnLoader.getFromExtensions(
        DruidModule.class
    );

    Assert.assertTrue(
        "modules contains TestDruidModule",
        Collections2.transform(modules, fnClassName).contains(TestDruidModule.class.getName())
    );
  }

  @Test
  public void test05MakeInjectorWithModules()
  {
    Injector startupInjector = startupInjector();
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        ImmutableSet.of(),
        ImmutableList.<com.google.inject.Module>of(
            new com.google.inject.Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test-inject", null, false, null, null, true, false)
                );
              }
            }
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertNotNull(ExtensionsLoader.instance(injector));
    Assert.assertSame(extnLoader, ExtensionsLoader.instance(injector));
  }

  @Test
  public void testCreateInjectorWithNodeRoles()
  {
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        ImmutableSet.of(new NodeRole("role1"), new NodeRole("role2")),
        ImmutableList.of(
            binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            )
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertEquals(expected, injector.getInstance(Key.get(DruidNode.class, Self.class)));
  }

  @Test
  public void testCreateInjectorWithEmptyNodeRolesAndRoleInjection()
  {
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        ImmutableSet.of(),
        ImmutableList.of(
            (com.google.inject.Module) new DruidModule()
            {
              @Inject
              public void setNodeRoles(@Self Set<NodeRole> nodeRoles)
              {
                Assert.assertTrue(nodeRoles.isEmpty());
              }

              @Override
              public void configure(Binder binder)
              {

              }
            },
            binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            )
        )
    );
    Assert.assertNotNull(injector);
  }

  @Test
  public void testCreateInjectorWithNodeRoleFilter_moduleNotLoaded()
  {
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        ImmutableSet.of(new NodeRole("role1"), new NodeRole("role2")),
        ImmutableList.of(
            (com.google.inject.Module) binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            ),
            new LoadOnAnnotationTestModule()
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertEquals(expected, injector.getInstance(Key.get(DruidNode.class, Self.class)));
    Assert.assertThrows(
        "Guice configuration errors",
        ConfigurationException.class,
        () -> injector.getInstance(Key.get(String.class, Names.named("emperor")))
    );
  }

  @Test
  public void testCreateInjectorWithNodeRoleFilterUsingAnnotation_moduleLoaded()
  {
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        ImmutableSet.of(new NodeRole("role1"), new NodeRole("druid")),
        ImmutableList.of(
            (com.google.inject.Module) binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            ),
            new LoadOnAnnotationTestModule()
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertEquals(expected, injector.getInstance(Key.get(DruidNode.class, Self.class)));
    Assert.assertEquals("I am Druid", injector.getInstance(Key.get(String.class, Names.named("emperor"))));
  }

  @LoadScope(roles = {"emperor", "druid"})
  private static class LoadOnAnnotationTestModule implements com.google.inject.Module
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(String.class).annotatedWith(Names.named("emperor")).toInstance("I am Druid");
    }
  }

  @Test
  public void testCreateInjectorWithNodeRoleFilterUsingInject_moduleNotLoaded()
  {
    final Set<NodeRole> nodeRoles = ImmutableSet.of(new NodeRole("role1"), new NodeRole("role2"));
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        nodeRoles,
        ImmutableList.of(
            (com.google.inject.Module) binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            ),
            new NodeRolesInjectTestModule()
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertEquals(expected, injector.getInstance(Key.get(DruidNode.class, Self.class)));
    Assert.assertThrows(
        "Guice configuration errors",
        ConfigurationException.class,
        () -> injector.getInstance(Key.get(String.class, Names.named("emperor")))
    );
  }

  @Test
  public void testCreateInjectorWithNodeRoleFilterUsingInject_moduleLoaded()
  {
    final Set<NodeRole> nodeRoles = ImmutableSet.of(new NodeRole("role1"), new NodeRole("druid"));
    final DruidNode expected = new DruidNode("test-inject", null, false, null, null, true, false);
    Injector startupInjector = startupInjector();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        startupInjector,
        nodeRoles,
        ImmutableList.of(
            (com.google.inject.Module) binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                expected
            ),
            new NodeRolesInjectTestModule()
        )
    );
    Assert.assertNotNull(injector);
    Assert.assertEquals(expected, injector.getInstance(Key.get(DruidNode.class, Self.class)));
    Assert.assertEquals("I am Druid", injector.getInstance(Key.get(String.class, Names.named("emperor"))));
  }

  private static class NodeRolesInjectTestModule implements com.google.inject.Module
  {
    private Set<NodeRole> nodeRoles;

    @Inject
    public void init(@Self Set<NodeRole> nodeRoles)
    {
      this.nodeRoles = nodeRoles;
    }

    @Override
    public void configure(Binder binder)
    {
      if (nodeRoles.contains(new NodeRole("emperor")) || nodeRoles.contains(new NodeRole("druid"))) {
        binder.bind(String.class).annotatedWith(Names.named("emperor")).toInstance("I am Druid");
      }
    }
  }
}
