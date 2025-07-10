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

package org.apache.druid.testing.embedded.emitter;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.LatchableEmitter;

/**
 * Guice module to use {@link LatchableEmitter}. This module is added to the file
 * {@code services/src/test/resources/META-INF/services/org.apache.druid.initialization.DruidModule}
 * so that it is registered as an extension.
 */
public class LatchableEmitterModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(Emitter.class, Names.named(LatchableEmitter.TYPE)))
          .to(LatchableEmitter.class);
  }

  @Provides
  @ManageLifecycle
  public LatchableEmitter makeEmitter(
      @Self DruidNode selfNode,
      ScheduledExecutorFactory executorFactory
  )
  {
    return new LatchableEmitter(selfNode.getServiceName(), selfNode.getHost(), executorFactory);
  }
}
