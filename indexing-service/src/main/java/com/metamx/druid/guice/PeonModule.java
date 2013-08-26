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

package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.metamx.druid.indexing.common.RetryPolicyConfig;
import com.metamx.druid.indexing.common.RetryPolicyFactory;
import com.metamx.druid.indexing.common.TaskToolboxFactory;
import com.metamx.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import com.metamx.druid.indexing.common.actions.TaskActionClientFactory;
import com.metamx.druid.indexing.common.config.TaskConfig;
import com.metamx.druid.indexing.common.index.ChatHandlerProvider;
import com.metamx.druid.indexing.common.index.EventReceivingChatHandlerProvider;
import com.metamx.druid.indexing.common.index.NoopChatHandlerProvider;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.S3DataSegmentKiller;

/**
 */
public class PeonModule implements Module
{
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
  }
}
