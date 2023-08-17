package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.apache.commons.lang.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface TaskPayloadManager {
  default void pushTaskPayload(String taskid, File taskPayloadFile) throws IOException {
    throw new NotImplementedException("Managing task payloads is not implemented for this druid.indexer.logs.type");
  }

  default Optional<InputStream> streamTaskPayload(String taskid) throws IOException {
    throw new NotImplementedException("Managing task payloads is not implemented for this druid.indexer.task.logs");
  }
}
