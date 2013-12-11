package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileTaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(FileTaskLogs.class);

  private final FileTaskLogsConfig config;

  @Inject
  public FileTaskLogs(
      FileTaskLogsConfig config
  )
  {
    this.config = config;
  }

  @Override
  public void pushTaskLog(final String taskid, File file) throws IOException
  {
    if (!config.getDirectory().exists()) {
      config.getDirectory().mkdir();
    }
    final File outputFile = fileForTask(taskid);
    Files.copy(file, outputFile);
    log.info("Wrote task log to: %s", outputFile);
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final File file = fileForTask(taskid);
    if (file.exists()) {
      return Optional.<InputSupplier<InputStream>>of(
          new InputSupplier<InputStream>()
          {
            @Override
            public InputStream getInput() throws IOException
            {
              final InputStream inputStream = new FileInputStream(file);
              ByteStreams.skipFully(inputStream, offset);
              return inputStream;
            }
          }
      );
    } else {
      return Optional.absent();
    }
  }

  private File fileForTask(final String taskid)
  {
    return new File(config.getDirectory(), String.format("%s.log", taskid));
  }
}
