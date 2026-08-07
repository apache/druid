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

package org.apache.druid.cli;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.server.initialization.jetty.JettyBindings;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class CliCoordinatorTest
{
  private static final String COORDINATOR_QOS_PATH = "/druid/coordinator/v1/*";

  @Test
  public void testQosFilterIsBoundByDefault()
  {
    final Injector injector = makeCoordinatorInjector(new Properties());

    final Set<JettyBindings.QosFilterHolder> qosFilters = getQosFilterHolders(injector);
    Assert.assertTrue(
        "Coordinator QoS filter should be bound when maxConcurrentRequests defaults to a positive value",
        hasCoordinatorQosFilter(qosFilters)
    );
  }

  @Test
  public void testQosFilterIsNotBoundWhenDisabled()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.coordinator.server.maxConcurrentRequests", "-1");
    final Injector injector = makeCoordinatorInjector(properties);

    final Set<JettyBindings.QosFilterHolder> qosFilters = getQosFilterHolders(injector);
    Assert.assertFalse(
        "Coordinator QoS filter should not be bound when maxConcurrentRequests is set to a non-positive value",
        hasCoordinatorQosFilter(qosFilters)
    );
  }

  @Test
  public void testLeaderEndpointsExcludedFromQos()
  {
    final Injector injector = makeCoordinatorInjector(new Properties());

    final JettyBindings.QosFilterHolder coordinatorQosFilter =
        getQosFilterHolders(injector).stream()
                                     .filter(holder -> Arrays.asList(holder.getPaths()).contains(COORDINATOR_QOS_PATH))
                                     .findFirst()
                                     .orElseThrow(() -> new AssertionError("Coordinator QoS filter should be bound"));


    final Set<String> excludedPaths = Set.of(coordinatorQosFilter.getExcludedPaths());
    Assert.assertTrue(
        "isLeader should be exempt from QoS filtering",
        excludedPaths.contains("/druid/coordinator/v1/isLeader")
    );
    Assert.assertTrue(
        "leader should be exempt from QoS filtering",
        excludedPaths.contains("/druid/coordinator/v1/leader")
    );
  }

  private static boolean hasCoordinatorQosFilter(Set<JettyBindings.QosFilterHolder> qosFilters)
  {
    return qosFilters.stream()
                     .anyMatch(holder -> Arrays.asList(holder.getPaths()).contains(COORDINATOR_QOS_PATH));
  }

  private static Set<JettyBindings.QosFilterHolder> getQosFilterHolders(Injector injector)
  {
    return injector.getInstance(Key.get(new TypeLiteral<Set<JettyBindings.QosFilterHolder>>() {}));
  }

  private static Injector makeCoordinatorInjector(final Properties props)
  {
    final Injector baseInjector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
        }
    );

    final CliCoordinator coordinator = new CliCoordinator();
    baseInjector.injectMembers(coordinator);
    return coordinator.makeInjector(Set.of(NodeRole.COORDINATOR));
  }
}
