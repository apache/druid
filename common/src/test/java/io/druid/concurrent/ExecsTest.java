package io.druid.concurrent;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecsTest
{
  @Test
  public void testBlockingExecutorService() throws Exception
  {
    final int capacity = 3;
    final ExecutorService executorService = Execs.blockingSingleThreaded("test%d", capacity);
    final AtomicInteger producedCount = new AtomicInteger();
    final AtomicInteger consumedCount = new AtomicInteger();
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Thread producer = new Thread("producer")
    {
      public void run()
      {
        for (int i = 0; i < 2 * capacity; i++) {
          final int taskID = i;
          System.out.println("Produced task"+ taskID);
          executorService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  System.out.println("Starting task" + taskID);
                  try {
                    barrier.await();
                  }
                  catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                  consumedCount.incrementAndGet();
                  System.out.println("Completed task" + taskID);
                }
              }
          );
          producedCount.incrementAndGet();
        }
      }
    };
    producer.start();
    for(int i=0;i<capacity;i++){
      Thread.sleep(500);
      // total completed tasks +1 running task+ capacity = total produced tasks
      Assert.assertEquals(consumedCount.intValue()+1+capacity,producedCount.intValue());
      barrier.await();

    }
    for(int i=0;i<capacity;i++){
      barrier.await();
    }


  }
}
