/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.index.ChatHandlerProvider;
import io.druid.indexing.common.index.EventReceivingChatHandlerProvider;
import io.druid.indexing.common.index.NoopChatHandlerProvider;
import io.druid.indexing.coordinator.TaskRunner;
import io.druid.indexing.coordinator.ThreadPoolTaskRunner;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.S3DataSegmentKiller;

/**
 */
public class PeonModule implements Module
{
  private final ExecutorLifecycleConfig config;

  public PeonModule(
      ExecutorLifecycleConfig config
  )
  {
    this.config = config;
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        "druid.indexer.task.chathandler.type",
        Key.get(ChatHandlerProvider.class),
        Key.get(NoopChatHandlerProvider.class)
    );
    final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
        binder, Key.get(ChatHandlerProvider.class)
    );
    handlerProviderBinder.addBinding("curator").to(EventReceivingChatHandlerProvider.class);
    handlerProviderBinder.addBinding("noop").to(NoopChatHandlerProvider.class);

    binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
    JsonConfigProvider.bind(binder, "druid.worker.taskActionClient.retry", RetryPolicyConfig.class);

    binder.bind(TaskActionClientFactory.class).to(RemoteTaskActionClientFactory.class).in(LazySingleton.class);
    binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);

    binder.bind(DataSegmentKiller.class).to(S3DataSegmentKiller.class).in(LazySingleton.class);

    binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
    binder.bind(ExecutorLifecycleConfig.class).toInstance(config);

    binder.bind(TaskRunner.class).to(ThreadPoolTaskRunner.class).in(LazySingleton.class);
    binder.bind(ThreadPoolTaskRunner.class).in(ManageLifecycle.class);
  }
}
