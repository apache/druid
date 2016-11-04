package io.druid.indexing.overlord;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;

import javax.annotation.Nullable;

public class PostingTierRemoteTaskRunner extends AbstractTierRemoteTaskRunner
{
  private static final Logger LOG = new Logger(PostingTierRemoteTaskRunner.class);

  @Inject
  public PostingTierRemoteTaskRunner(
      TierTaskDiscovery tierTaskDiscovery,
      @Global HttpClient httpClient,
      TaskStorage taskStorage,
      ScheduledExecutorFactory executorFactory
  )
  {
    super(tierTaskDiscovery, httpClient, taskStorage, executorFactory);
  }

  @Override
  protected void launch(SettableFuture future, Task task)
  {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  protected ListenableFuture<?> kill(final DruidNode node)
  {
    final ListenableFuture<?> softKillFuture = super.kill(node);
    final SettableFuture<?> hardKillFuture = SettableFuture.create();
    Futures.addCallback(softKillFuture, new FutureCallback<Object>()
    {
      @Override
      public void onSuccess(@Nullable Object result)
      {
        LOG.debug("Shutdown request on [%s] succeeded", node);
      }

      @Override
      public void onFailure(Throwable t)
      {
        LOG.warn(t, "Shutdown request on [%s] failed, attempting hard-kill");
      }
    });
    return hardKillFuture;
  }
}
