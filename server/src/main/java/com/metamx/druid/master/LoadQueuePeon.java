package com.metamx.druid.master;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.coordination.DataSegmentChangeRequest;
import com.metamx.druid.coordination.SegmentChangeRequestDrop;
import com.metamx.druid.coordination.SegmentChangeRequestLoad;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LoadQueuePeon implements PhoneBookPeon<Map>
{
  private static final Logger log = new Logger(LoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;

  private final Object lock = new Object();

  private final PhoneBook yp;
  private final String basePath;
  private final ScheduledExecutorService zkWritingExecutor;

  private final AtomicLong queuedSize = new AtomicLong(0);

  private static Comparator<SegmentHolder> segmentHolderComparator = new Comparator<SegmentHolder>()
  {
    private Comparator<DataSegment> comparator = Comparators.inverse(DataSegment.bucketMonthComparator());

    @Override
    public int compare(SegmentHolder lhs, SegmentHolder rhs)
    {
      return comparator.compare(lhs.getSegment(), rhs.getSegment());
    }
  };

  private final ConcurrentSkipListSet<SegmentHolder> segmentsToLoad = new ConcurrentSkipListSet<SegmentHolder>(
      segmentHolderComparator
  );
  private final ConcurrentSkipListSet<SegmentHolder> segmentsToDrop = new ConcurrentSkipListSet<SegmentHolder>(
      segmentHolderComparator
  );

  private volatile SegmentHolder currentlyLoading = null;

  LoadQueuePeon(
      PhoneBook yp,
      String basePath,
      ScheduledExecutorService zkWritingExecutor
  )
  {
    this.yp = yp;
    this.basePath = basePath;
    this.zkWritingExecutor = zkWritingExecutor;
  }

  @Override
  public Class<Map> getObjectClazz()
  {
    return Map.class;
  }

  @Override
  public void newEntry(String name, Map properties)
  {
    synchronized (lock) {
      if (currentlyLoading == null) {
        log.warn(
            "Server[%s] a new entry[%s] appeared, even though nothing is currently loading[%s]",
            basePath,
            name,
            currentlyLoading
        );
      } else {
        if (!name.equals(currentlyLoading.getSegmentIdentifier())) {
          log.warn(
              "Server[%s] a new entry[%s] appeared that is not the currently loading entry[%s]",
              basePath,
              name,
              currentlyLoading
          );
        } else {
          log.info("Server[%s]'s currently loading entry[%s] appeared.", basePath, name);
        }
      }
    }
  }

  @Override
  public void entryRemoved(String name)
  {
    synchronized (lock) {
      if (currentlyLoading == null) {
        log.warn("Server[%s] an entry[%s] was removed even though it wasn't loading!?", basePath, name);
        return;
      }
      if (!name.equals(currentlyLoading.getSegmentIdentifier())) {
        log.warn(
            "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
            basePath,
            name,
            currentlyLoading
        );
        return;
      }
      actionCompleted();
      log.info("Server[%s] done processing [%s]", basePath, name);
    }

    doNext();
  }

  public Set<DataSegment> getSegmentsToLoad()
  {
    return new ConcurrentSkipListSet<DataSegment>(
        Collections2.transform(
            segmentsToLoad,
            new Function<SegmentHolder, DataSegment>()
            {
              @Override
              public DataSegment apply(SegmentHolder input)
              {
                return input.getSegment();
              }
            }
        )
    );
  }

  public Set<DataSegment> getSegmentsToDrop()
  {
    return new ConcurrentSkipListSet<DataSegment>(
        Collections2.transform(
            segmentsToDrop,
            new Function<SegmentHolder, DataSegment>()
            {
              @Override
              public DataSegment apply(SegmentHolder input)
              {
                return input.getSegment();
              }
            }
        )
    );
  }

  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  public void loadSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyLoading != null) &&
          currentlyLoading.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    SegmentHolder holder = new SegmentHolder(segment, LOAD, Arrays.asList(callback));

    synchronized (lock) {
      if (segmentsToLoad.contains(holder)) {
        if ((callback != null)) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to load segment[%s]", basePath, segment);
    queuedSize.addAndGet(segment.getSize());
    segmentsToLoad.add(holder);
    doNext();
  }

  public void dropSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyLoading != null) &&
          currentlyLoading.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    SegmentHolder holder = new SegmentHolder(segment, DROP, Arrays.asList(callback));

    synchronized (lock) {
      if (segmentsToDrop.contains(holder)) {
        if (callback != null) {
          currentlyLoading.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server peon[%s] to drop segment[%s]", basePath, segment);
    segmentsToDrop.add(holder);
    doNext();
  }

  private void doNext()
  {
    synchronized (lock) {
      if (currentlyLoading == null) {
        if (!segmentsToDrop.isEmpty()) {
          currentlyLoading = segmentsToDrop.first();
          log.info("Server[%s] dropping [%s]", basePath, currentlyLoading);
        } else if (!segmentsToLoad.isEmpty()) {
          currentlyLoading = segmentsToLoad.first();
          log.info("Server[%s] loading [%s]", basePath, currentlyLoading);
        } else {
          return;
        }

        submitExecutable();
      } else {
        log.info(
            "Server[%s] skipping doNext() because something is currently loading[%s].", basePath, currentlyLoading
        );
      }
    }
  }

  private void submitExecutable()
  {
    final SegmentHolder currentlyLoadingRef = currentlyLoading;
    final AtomicBoolean postedEphemeral = new AtomicBoolean(false);
    zkWritingExecutor.execute(
        new Runnable()
        {
          @Override
          public void run()
          {
            synchronized (lock) {
              try {
                if (currentlyLoading == null) {
                  log.error("Crazy race condition! server[%s]", basePath);
                  postedEphemeral.set(true);
                  actionCompleted();
                  doNext();
                  return;
                }
                log.info("Server[%s] adding segment[%s]", basePath, currentlyLoading.getSegmentIdentifier());
                yp.postEphemeral(
                    basePath,
                    currentlyLoading.getSegmentIdentifier(),
                    currentlyLoading.getChangeRequest()
                );
                postedEphemeral.set(true);
              }
              catch (Throwable e) {
                log.error(e, "Server[%s], throwable caught when submitting [%s].", basePath, currentlyLoading);
                // Act like it was completed so that the master gives it to someone else
                postedEphemeral.set(true);
                actionCompleted();
                doNext();
              }
            }
          }
        }
    );
    zkWritingExecutor.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            synchronized (lock) {
              String path = yp.combineParts(Arrays.asList(basePath, currentlyLoadingRef.getSegmentIdentifier()));

              if (!postedEphemeral.get()) {
                log.info("Ephemeral hasn't been posted yet for [%s], rescheduling.", path);
                zkWritingExecutor.schedule(this, 60, TimeUnit.SECONDS);
              }
              if (currentlyLoadingRef == currentlyLoading) {
                if (yp.lookup(path, Object.class) == null) {
                  log.info("Looks like [%s] was created and deleted without the watchers finding out.", path);
                  entryRemoved(currentlyLoadingRef.getSegmentIdentifier());
                } else {
                  log.info("Path[%s] still out on ZK, rescheduling.", path);
                  zkWritingExecutor.schedule(this, 60, TimeUnit.SECONDS);
                }
              }
            }
          }
        },
        60,
        TimeUnit.SECONDS
    );
  }

  private void actionCompleted()
  {
    if (currentlyLoading != null) {
      switch (currentlyLoading.getType()) {
        case LOAD:
          segmentsToLoad.remove(currentlyLoading);
          queuedSize.addAndGet(-currentlyLoading.getSegmentSize());
          break;
        case DROP:
          segmentsToDrop.remove(currentlyLoading);
          break;
        default:
          throw new UnsupportedOperationException();
      }
      currentlyLoading.executeCallbacks();
      currentlyLoading = null;
    }
  }

  public void stop()
  {
    synchronized (lock) {
      if (currentlyLoading != null) {
        currentlyLoading.executeCallbacks();
        currentlyLoading = null;
      }

      if (!segmentsToDrop.isEmpty()) {
        for (SegmentHolder holder : segmentsToDrop) {
          holder.executeCallbacks();
        }
      }
      segmentsToDrop.clear();

      if (!segmentsToLoad.isEmpty()) {
        for (SegmentHolder holder : segmentsToLoad) {
          holder.executeCallbacks();
        }
      }
      segmentsToLoad.clear();

      queuedSize.set(0L);
    }
  }

  private class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final int type;
    private final List<LoadPeonCallback> callbacks = Lists.newArrayList();

    private SegmentHolder(
        DataSegment segment,
        int type,
        Collection<LoadPeonCallback> callbacks
    )
    {
      this.segment = segment;
      this.type = type;
      this.changeRequest = (type == LOAD)
                           ? new SegmentChangeRequestLoad(segment)
                           : new SegmentChangeRequestDrop(segment);
      this.callbacks.addAll(callbacks);
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public int getType()
    {
      return type;
    }

    public String getSegmentIdentifier()
    {
      return segment.getIdentifier();
    }

    public long getSegmentSize()
    {
      return segment.getSize();
    }

    public void addCallbacks(Collection<LoadPeonCallback> newCallbacks)
    {
      synchronized (callbacks) {
        callbacks.addAll(newCallbacks);
      }
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        callbacks.add(newCallback);
      }
    }

    public void executeCallbacks()
    {
      synchronized (callbacks) {
        for (LoadPeonCallback callback : callbacks) {
          if (callback != null) {
            callback.execute();
          }
        }
        callbacks.clear();
      }
    }

    public DataSegmentChangeRequest getChangeRequest()
    {
      return changeRequest;
    }

    @Override
    public String toString()
    {
      return changeRequest.toString();
    }
  }
}