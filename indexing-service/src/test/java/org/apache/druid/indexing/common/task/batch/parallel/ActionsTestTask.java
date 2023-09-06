package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.task.CommandQueueTask;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

/**
 * Test task that can only invoke task actions.
 */
public class ActionsTestTask extends CommandQueueTask
{
  private final TaskActionClient client;

  public ActionsTestTask(String datasource, TaskActionClientFactory factory)
  {
    super(datasource, null);
    this.client = factory.create(this);
  }

  public TaskLock acquireReplaceLockOn(Interval interval)
  {
    return tryTimeChunkLock(interval, TaskLockType.REPLACE);
  }

  public TaskLock acquireAppendLockOn(Interval interval)
  {
    return tryTimeChunkLock(interval, TaskLockType.APPEND);
  }

  public SegmentPublishResult commitReplaceSegments(DataSegment... segments)
  {
    return runAction(
        SegmentTransactionalReplaceAction.create(Sets.newHashSet(segments))
    );
  }

  public SegmentPublishResult commitAppendSegments(DataSegment... segments)
  {
    return runAction(
        SegmentTransactionalAppendAction.create(Sets.newHashSet(segments))
    );
  }

  public SegmentIdWithShardSpec allocateSegment()
  {
    return null;
  }

  private TaskLock tryTimeChunkLock(Interval interval, TaskLockType lockType)
  {
    final TaskLock lock = runAction(new TimeChunkLockTryAcquireAction(lockType, interval));
    if (lock == null) {
      throw new ISE("Could not acquire [%s] lock on interval[%s] for task[%s]", lockType, interval, getId());
    } else if (lock.isRevoked()) {
      throw new ISE("Acquired [%s] lock on interval[%s] for task[%s] has been revoked.", lockType, interval, getId());
    }

    return lock;
  }

  private <T> T runAction(TaskAction<T> action)
  {
    return execute(() -> client.submit(action));
  }
}
