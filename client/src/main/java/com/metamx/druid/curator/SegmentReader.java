package com.metamx.druid.curator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;

/**
 */
public class SegmentReader
{
  private static final Logger log = new Logger(SegmentReader.class);

  private final CuratorFramework cf;
  private final ObjectMapper jsonMapper;

  public SegmentReader(CuratorFramework cf, ObjectMapper jsonMapper)
  {
    this.cf = cf;
    this.jsonMapper = jsonMapper;
  }

  public Set<DataSegment> read(String path)
  {
    try {
      if (cf.checkExists().forPath(path) != null) {
        return jsonMapper.readValue(
            cf.getData().forPath(path), new TypeReference<Set<DataSegment>>()
        {
        }
        );
      }
    }
    catch (Exception e) {
      log.error("Unable to read any segment ids from %s", path);
      throw Throwables.propagate(e);
    }

    return Sets.newHashSet();
  }
}
