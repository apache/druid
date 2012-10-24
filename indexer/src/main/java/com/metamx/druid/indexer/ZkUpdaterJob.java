package com.metamx.druid.indexer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexer.updater.ZkUpdaterJobSpec;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ZkClientConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

/**
 */
public class ZkUpdaterJob implements Jobby
{
  private static final Logger log = new Logger(ZkUpdaterJob.class);

  private static final Joiner JOINER = Joiner.on("/");
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final HadoopDruidIndexerConfig config;
  private final ZkUpdaterJobSpec spec;

  public ZkUpdaterJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.spec = (ZkUpdaterJobSpec) config.getUpdaterJobSpec();
  }

  @Override
  public boolean run()
  {
    if (!spec.postToZk()) {
      return true;
    }

    Configuration conf = new Configuration();

    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }

    final Lifecycle lifecycle = new Lifecycle();
    ZkClient zkClient = Initialization.makeZkClient(
        new ZkClientConfig()
        {
          @Override
          public String getZkHosts()
          {
            return spec.getZkQuorum();
          }
        },
        lifecycle
    );

    try {
      lifecycle.start();
    }
    catch (Exception e) {
      log.error(e, "Exception on lifecycle start?");
      lifecycle.stop();
      return false;
    }

    try {
      zkClient.waitUntilConnected();
      final String dataSourceBasePath = JOINER.join(spec.getZkBasePath(), config.getDataSource());
      if (! zkClient.exists(dataSourceBasePath)) {
        zkClient.createPersistent(
            dataSourceBasePath,
            jsonMapper.writeValueAsString(
                ImmutableMap.of(
                    "created", new DateTime().toString()
                )
            )
        );
      }

      for (Bucket bucket: config.getAllBuckets()) {
        final Path path = new Path(config.makeSegmentOutputPath(bucket), "descriptor.json");
        DataSegment segment = jsonMapper.readValue(
            path.getFileSystem(conf).open(path),
            DataSegment.class
        );

        String segmentPath = JOINER.join(dataSourceBasePath, segment.getIdentifier());
        log.info("Adding index to list of indexes at zkPath[%s].", segmentPath);
        zkClient.createPersistent(segmentPath, jsonMapper.writeValueAsString(segment));
      }
    }
    catch (Exception e) {
      log.error(e, "Exception when trying to update zk metadata.");
      return false;
    }
    finally {
      lifecycle.stop();
    }

    return true;
  }
}
