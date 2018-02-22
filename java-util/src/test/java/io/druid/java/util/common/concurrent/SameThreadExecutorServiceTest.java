package io.druid.java.util.common.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class SameThreadExecutorServiceTest
{
  @Test
  public void timeoutAndShutdownTest() throws Exception
  {
    final SameThreadExecutorService service = new SameThreadExecutorService();
    Assert.assertFalse(service.awaitTermination(10, TimeUnit.MILLISECONDS));
    service.shutdown();
    Assert.assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void hangingTaskTest() throws Exception
  {
    final CountDownLatch latch = new CountDownLatch(1);
    final SameThreadExecutorService service = new SameThreadExecutorService();
    // Runnable gets called on submit, use the common pool to do the submit action
    ForkJoinPool.commonPool().submit(
        () -> service.submit(() -> {
          try {
            latch.await();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
    );
    Assert.assertFalse(service.awaitTermination(10, TimeUnit.MILLISECONDS));
    service.shutdown();
    Assert.assertFalse(service.awaitTermination(10, TimeUnit.MILLISECONDS));
    latch.countDown();
    Assert.assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
    Assert.assertTrue(service.isTerminated());
  }

  @Test
  public void testMultiShutdownIsFine() {
    final SameThreadExecutorService service = new SameThreadExecutorService();
    Assert.assertFalse(service.isShutdown());
    service.shutdown();
    Assert.assertTrue(service.isShutdown());
    service.shutdown();
    Assert.assertTrue(service.isShutdown());
  }
}
