package org.apache.druid.tasklogs;

import com.google.common.base.Optional;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface TaskPayloadManager {
  void pushTaskPayload(String taskid, File taskPayloadFile) throws IOException;

  Optional<InputStream> streamTaskPayload(String taskid) throws IOException;
}
