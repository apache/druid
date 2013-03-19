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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class ExecutorMain
{
  private static final Logger log = new Logger(ExecutorMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    if (args.length != 2) {
      log.info("Usage: ExecutorMain <task.json> <status.json>");
      System.exit(2);
    }

    final ExecutorNode node = ExecutorNode.builder().build();
    final Lifecycle lifecycle = new Lifecycle();

    lifecycle.addManagedInstance(node);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      System.exit(2);
    }

    // Spawn monitor thread to keep a watch on our parent process
    // If stdin reaches eof, the parent is gone, and we should shut down
    final ExecutorService parentMonitorExec = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("parent-monitor-%d")
            .setDaemon(true)
            .build()
    );

    parentMonitorExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            int b = -1;
            try {
              b = System.in.read();
            } catch (Exception e) {
              log.error(e, "Failed to read from stdin");
            }

            // TODO ugh this is gross
            System.exit(2);
          }
        }
    );

    try {
      final Task task = node.getJsonMapper().readValue(new File(args[0]), Task.class);

      log.info(
          "Running with task: %s",
          node.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(task)
      );

      final TaskStatus status = node.run(task).get();

      log.info(
          "Task completed with status: %s",
          node.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(status)
      );

      node.getJsonMapper().writeValue(new File(args[1]), status);
    } finally {
      lifecycle.stop();
    }

    // TODO maybe this shouldn't be needed?
    System.exit(0);
  }
}
