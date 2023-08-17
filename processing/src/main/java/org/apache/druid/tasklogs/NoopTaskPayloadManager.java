package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class NoopTaskPayloadManager implements TaskPayloadManager {
  private final Logger log = new Logger(TaskPayloadManager.class);
  @Override
  public void pushTaskPayload(String taskid, File taskPayloadFile) throws IOException {
    log.info("Not pushing task payload for task: %s", taskid);
  }

  @Override
  public Optional<InputStream> streamTaskPayload(String taskid) throws IOException {
    return Optional.absent();
  }
}
