/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class ZkCoordinator implements DataSegmentChangeHandler
{
  private static final Logger log = new Logger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final ZkCoordinatorConfig config;
  private final DruidServer me;
  private final PhoneBook yp;
  private final ServerManager serverManager;
  private final ServiceEmitter emitter;
  private final List<Pair<String, PhoneBookPeon<?>>> peons;

  private final String loadQueueLocation;
  private final String servedSegmentsLocation;

  private volatile boolean started;

  public ZkCoordinator(
      ObjectMapper jsonMapper,
      ZkCoordinatorConfig config,
      DruidServer me,
      PhoneBook yp,
      ServerManager serverManager,
      ServiceEmitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.me = me;
    this.yp = yp;
    this.serverManager = serverManager;
    this.emitter = emitter;

    this.peons = new ArrayList<Pair<String, PhoneBookPeon<?>>>();
    this.loadQueueLocation = yp.combineParts(Arrays.asList(config.getLoadQueueLocation(), me.getName()));
    this.servedSegmentsLocation = yp.combineParts(
        Arrays.asList(
            config.getServedSegmentsLocation(), me.getName()
        )
    );

    this.config.getSegmentInfoCacheDirectory().mkdirs();
  }

  @LifecycleStart
  public void start() throws IOException
  {
    log.info("Starting zkCoordinator for server[%s] with config[%s]", me, config);
    synchronized (lock) {
      if (started) {
        return;
      }

      if (yp.lookup(loadQueueLocation, Object.class) == null) {
        yp.post(
            config.getLoadQueueLocation(),
            me.getName(),
            ImmutableMap.of("created", new DateTime().toString())
        );
      }
      if (yp.lookup(servedSegmentsLocation, Object.class) == null) {
        yp.post(
            config.getServedSegmentsLocation(),
            me.getName(),
            ImmutableMap.of("created", new DateTime().toString())
        );
      }

      loadCache();

      yp.announce(
          config.getAnnounceLocation(),
          me.getName(),
          me.getStringProps()
      );

      peons.add(
          new Pair<String, PhoneBookPeon<?>>(
              loadQueueLocation,
              new PhoneBookPeon<DataSegmentChangeRequest>()
              {
                @Override
                public Class<DataSegmentChangeRequest> getObjectClazz()
                {
                  return DataSegmentChangeRequest.class;
                }

                @Override
                public void newEntry(String nodeName, DataSegmentChangeRequest segment)
                {
                  log.info("New node[%s] with segmentClass[%s]", nodeName, segment.getClass());

                  try {
                    segment.go(ZkCoordinator.this);
                    yp.unpost(loadQueueLocation, nodeName);

                    log.info("Completed processing for node[%s]", nodeName);
                  }
                  catch (Throwable t) {
                    yp.unpost(loadQueueLocation, nodeName);

                    log.error(
                        t, "Uncaught throwable made it through loading.  Node[%s/%s]", loadQueueLocation, nodeName
                    );
                    Map<String, Object> exceptionMap = Maps.newHashMap();
                    exceptionMap.put("node", loadQueueLocation);
                    exceptionMap.put("nodeName", nodeName);
                    exceptionMap.put("nodeProperties", segment.toString());
                    exceptionMap.put("exception", t.getMessage());
                    emitter.emit(
                        new AlertEvent.Builder().build(
                            "Uncaught exception related to segment load/unload",
                            exceptionMap
                        )
                    );

                    throw Throwables.propagate(t);
                  }
                }

                @Override
                public void entryRemoved(String name)
                {
                  log.info("%s was removed", name);
                }
              }
          )
      );

      for (Pair<String, PhoneBookPeon<?>> peon : peons) {
        yp.registerListener(peon.lhs, peon.rhs);
      }

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping ZkCoordinator with config[%s]", config);
    synchronized (lock) {
      if (!started) {
        return;
      }

      for (Pair<String, PhoneBookPeon<?>> peon : peons) {
        yp.unregisterListener(peon.lhs, peon.rhs);
      }
      peons.clear();

      yp.unannounce(config.getAnnounceLocation(), me.getName());

      started = false;
    }
  }

  private void loadCache()
  {
    File baseDir = config.getSegmentInfoCacheDirectory();
    if (!baseDir.exists()) {
      return;
    }

    for (File file : baseDir.listFiles()) {
      log.info("Loading segment cache file [%s]", file);
      try {
        DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        if (serverManager.isSegmentCached(segment)) {
          addSegment(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getIdentifier());

          File segmentInfoCacheFile = new File(config.getSegmentInfoCacheDirectory(), segment.getIdentifier());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
      catch (Exception e) {
        log.error(e, "Exception occurred reading file [%s]", file);
        emitter.emit(
            new AlertEvent.Builder().build(
                "Failed to read segment info file",
                ImmutableMap.<String, Object>builder()
                            .put("file", file)
                            .put("exception", e.toString())
                            .build()
            )
        );
      }
    }
  }

  @Override
  public void addSegment(DataSegment segment)
  {
    try {
      serverManager.loadSegment(segment);

      File segmentInfoCacheFile = new File(config.getSegmentInfoCacheDirectory(), segment.getIdentifier());
      try {
        jsonMapper.writeValue(segmentInfoCacheFile, segment);
      }
      catch (IOException e) {
        removeSegment(segment);
        throw new SegmentLoadingException(
            "Failed to write to disk segment info cache file[%s]", segmentInfoCacheFile
        );
      }

      yp.announce(servedSegmentsLocation, segment.getIdentifier(), segment);
    }
    catch (SegmentLoadingException e) {
      log.error(e, "Failed to load segment[%s]", segment);
      emitter.emit(
          new AlertEvent.Builder().build(
              "Failed to load segment",
              ImmutableMap.<String, Object>builder()
                          .put("segment", segment.toString())
                          .put("exception", e.toString())
                          .build()
          )
      );
    }
  }

  @Override
  public void removeSegment(DataSegment segment)
  {
    try {
      serverManager.dropSegment(segment);

      File segmentInfoCacheFile = new File(config.getSegmentInfoCacheDirectory(), segment.getIdentifier());
      if (!segmentInfoCacheFile.delete()) {
        log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
      }

      yp.unannounce(servedSegmentsLocation, segment.getIdentifier());
    }
    catch (Exception e) {
      log.error(e, "Exception thrown when dropping segment[%s]", segment);
      emitter.emit(
          new AlertEvent.Builder().build(
              "Failed to remove segment",
              ImmutableMap.<String, Object>builder()
                          .put("segment", segment.toString())
                          .put("exception", e.toString())
                          .build()
          )
      );
    }
  }
}
