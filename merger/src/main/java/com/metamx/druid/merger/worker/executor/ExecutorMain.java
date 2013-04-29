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

package com.metamx.druid.merger.worker.executor;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

/**
 */
public class ExecutorMain
{
  private static final Logger log = new Logger(ExecutorMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    if (args.length != 3) {
      log.info("Usage: ExecutorMain <task.json> <status.json>");
      System.exit(2);
    }

    Iterator<String> arguments = Arrays.asList(args).iterator();
    final String taskJsonFile = arguments.next();
    final String statusJsonFile = arguments.next();

    final ExecutorNode node = ExecutorNode.builder()
                                          .build(
                                              System.getProperty("druid.executor.nodeType", "indexer-executor"),
                                              new ExecutorLifecycleFactory(
                                                  new File(taskJsonFile),
                                                  new File(statusJsonFile),
                                                  System.in
                                              )
                                          );

    final Lifecycle lifecycle = new Lifecycle();

    lifecycle.addManagedInstance(node);

    try {
      lifecycle.start();
      node.join();
      lifecycle.stop();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      System.exit(2);
    }
  }
}
