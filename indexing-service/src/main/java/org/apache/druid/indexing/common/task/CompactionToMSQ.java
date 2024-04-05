package org.apache.druid.indexing.common.task;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.Interval;

import java.util.List;

public interface CompactionToMSQ
{
    TaskStatus createAndRunMSQControllerTasks(
        CompactionTask compactionTask,
        TaskToolbox taskToolbox,
        List<Pair<Interval, DataSchema>> dataSchemas
    ) throws Exception;

}
