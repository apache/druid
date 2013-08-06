package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.druid.client.InventoryView;
import com.metamx.druid.client.ServerInventoryView;
import com.metamx.druid.client.ServerInventoryViewProvider;
import com.metamx.druid.client.ServerView;

/**
 */
public class ServerViewModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.announcer", ServerInventoryViewProvider.class);
    binder.bind(InventoryView.class).to(ServerInventoryView.class);
    binder.bind(ServerView.class).to(ServerInventoryView.class);
    binder.bind(ServerInventoryView.class).toProvider(ServerInventoryViewProvider.class).in(ManageLifecycle.class);
  }
}
