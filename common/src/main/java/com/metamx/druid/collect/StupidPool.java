package com.metamx.druid.collect;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.LinkedList;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final LinkedList<T> objects = Lists.newLinkedList();

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
  }

  public ResourceHolder<T> take()
  {
    synchronized (objects) {
      if (objects.size() > 0) {
        return new ObjectResourceHolder(objects.removeFirst());
      }
    }

    return new ObjectResourceHolder(generator.get());
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private boolean closed = false;
    private final T object;

    public ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    @Override
    public synchronized T get()
    {
      if (closed) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public synchronized void close() throws IOException
    {
      if (closed) {
        log.warn(new ISE("Already Closed!"), "Already closed");
        return;
      }

      synchronized (objects) {
        closed = true;
        objects.addLast(object);
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      if (! closed) {
        log.warn("Not closed!  Object was[%s]. Allowing gc to prevent leak.", object);
      }
    }
  }
}
