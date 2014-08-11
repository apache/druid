package io.druid.segment.loading;

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Map;

/**
 */
public class LocalDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(LocalDataSegmentKiller.class);

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final File path = getDirectory(segment);
    log.info("segment[%s] maps to path[%s]", segment.getIdentifier(), path);

    if (!path.isDirectory()) {
      if (!path.delete()) {
        log.error("Unable to delete file[%s].", path);
        throw new SegmentLoadingException("Couldn't kill segment[%s]", segment.getIdentifier());
      }

      return;
    }

    final File[] files = path.listFiles();
    int success = 0;

    for (File file : files) {
      if (!file.delete()) {
        log.error("Unable to delete file[%s].", file);
      } else {
        ++success;
      }
    }

    if (success == 0 && files.length != 0) {
      throw new SegmentLoadingException("Couldn't kill segment[%s]", segment.getIdentifier());
    }

    if (success < files.length) {
      log.warn("Couldn't completely kill segment[%s]", segment.getIdentifier());
    } else if (!path.delete()) {
      log.warn("Unable to delete directory[%s].", path);
      log.warn("Couldn't completely kill segment[%s]", segment.getIdentifier());
    }
  }

  private File getDirectory(DataSegment segment) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final File path = new File(MapUtils.getString(loadSpec, "path"));

    if (!path.exists()) {
      throw new SegmentLoadingException("Asked to load path[%s], but it doesn't exist.", path);
    }

    return path.getParentFile();
  }
}
