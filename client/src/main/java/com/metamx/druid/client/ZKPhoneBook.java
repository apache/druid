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

package com.metamx.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.metamx.common.logger.Logger;
import com.metamx.phonebook.BasePhoneBook;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 */
public class ZKPhoneBook extends BasePhoneBook
{
  private static final Logger log = new Logger(ZKPhoneBook.class);

  public ZKPhoneBook(
      final ObjectMapper jsonMapper,
      final ZkClient zkClient,
      final Executor peonExecutor
  )
  {
    super(
        new InternalPhoneBook(jsonMapper, zkClient, peonExecutor)
    );
  }

  private static class InternalPhoneBook implements PhoneBook
  {
    private static final Joiner JOINER = Joiner.on("/");

    private final Object lock = new Object();

    private final Table<String, PhoneBookPeon, IZkChildListener> listeners;
    private final Table<String, String, Object> announcements;
    private final Map<String, PhoneBookPeon> announcementListeners;

    private final ObjectMapper jsonMapper;
    private final ZkClient zkClient;
    private final Executor exec;

    public InternalPhoneBook(
        ObjectMapper jsonMapper,
        ZkClient zkClient,
        Executor exec
    )
    {
      this.jsonMapper = jsonMapper;
      this.zkClient = zkClient;
      this.exec = exec;
      listeners = HashBasedTable.create();
      announcements = HashBasedTable.create();
      announcementListeners = Maps.newHashMap();
    }

    @Override
    public void start()
    {

    }

    @Override
    public void stop()
    {
      synchronized (lock) {
        for (Map.Entry<String, PhoneBookPeon> entry : announcementListeners.entrySet()) {
          unregisterListener(entry.getKey(), entry.getValue());
        }

        for (Table.Cell<String, String, Object> cell : announcements.cellSet()) {
          unannounce(cell.getRowKey(), cell.getColumnKey());
        }
      }
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public <T> void announce(final String serviceName, String nodeName, T properties)
    {
      if (!zkClient.exists(serviceName)) {
        zkClient.createPersistent(serviceName, true);
      }

      try {
        synchronized (lock) {
          zkClient.createEphemeral(
              getPath(serviceName, nodeName),
              jsonMapper.writeValueAsString(properties)
          );

          PhoneBookPeon peon = announcementListeners.get(serviceName);

          if (peon == null) {
            peon = new PhoneBookPeon<Object>()
            {
              @Override
              public Class<Object> getObjectClazz()
              {
                return Object.class;
              }

              @Override
              public void newEntry(String name, Object properties)
              {
              }

              @Override
              public void entryRemoved(String name)
              {
                synchronized (lock) {
                  Object propertyMap = announcements.get(serviceName, name);
                  if (propertyMap != null) {
                    log.info("entry[%s/%s] was removed but I'm in charge of it, reinstating.", serviceName, name);

                    String path = getPath(serviceName, name);
                    try {
                      zkClient.createEphemeral(
                          path,
                          jsonMapper.writeValueAsString(propertyMap)
                      );
                    }
                    catch (ZkNodeExistsException e) {
                      log.info("Thought that [%s] didn't exist, but it did?", path);
                    }
                    catch (IOException e) {
                      log.error(e, "Exception thrown when recreating node[%s].", path);
                    }
                  }
                }
              }
            };

            announcementListeners.put(serviceName, peon);
            registerListener(serviceName, peon);
          }

          announcements.put(serviceName, nodeName, properties);
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void unannounce(String serviceName, String nodeName)
    {
      synchronized (lock) {
        Object announcementMap = announcements.remove(serviceName, nodeName);
        Map<String, Object> storedProperties = lookup(combineParts(Arrays.asList(serviceName, nodeName)), Map.class);

        if (announcementMap == null || storedProperties == null) {
          return;
        }

        //Hack to compute equality because jsonMapper doesn't actually give me a Map<String, String> :(
        boolean areEqual = false;
        try {
          areEqual = storedProperties.equals(
              jsonMapper.readValue(jsonMapper.writeValueAsString(announcementMap), Map.class)
          );
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        
        log.debug("equal?[%s]: announcementMap[%s], storedProperties[%s].", areEqual, announcementMap, storedProperties);
        if (areEqual) {
          zkClient.delete(getPath(serviceName, nodeName));
        }
      }
    }

    @Override
    public <T> T lookup(String path, Class<? extends T> clazz)
    {
      final String nodeContent;
      try {
        nodeContent = zkClient.readData(path).toString();
      }
      catch (ZkNoNodeException e) {
        return null;
      }

      try {
        return jsonMapper.readValue(nodeContent, clazz);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <T> void post(String serviceName, String nodeName, final T properties)
    {
      if (!zkClient.exists(serviceName)) {
        zkClient.createPersistent(serviceName);
      }

      final String path = getPath(serviceName, nodeName);
      if (zkClient.exists(path)) {
        zkClient.updateDataSerialized(
            path,
            new DataUpdater<Object>()
            {
              @Override
              public Object update(Object currentData)
              {
                try {
                  return jsonMapper.writeValueAsString(properties);
                }
                catch (IOException e) {
                  log.error(e, "Exception when updating value of [%s].  Using currentData.", path);
                  return currentData;
                }
              }
            }
        );
      }

      try {
        zkClient.createPersistent(path, jsonMapper.writeValueAsString(properties));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean unpost(String serviceName, String nodeName)
    {
      return zkClient.delete(getPath(serviceName, nodeName));
    }

    @Override
    public <T> void postEphemeral(final String serviceName, String nodeName, T properties)
    {
      if (!zkClient.exists(serviceName)) {
        zkClient.createPersistent(serviceName, true);
      }

      try {
        zkClient.createEphemeral(
            getPath(serviceName, nodeName),
            jsonMapper.writeValueAsString(properties)
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <T> void registerListener(final String serviceName, final PhoneBookPeon<T> peon)
    {
      final Set<String> currChildren = Sets.newHashSet();

      IZkChildListener peonListener = new IZkChildListener()
      {
        @Override
        public void handleChildChange(final String parentPath, final List<String> incomingChildren) throws Exception
        {
          exec.execute(
              new InternalPhoneBook.UpdatingRunnable<T>(
                  parentPath,
                  currChildren,
                  (incomingChildren == null ? Sets.<String>newHashSet() : Sets.<String>newHashSet(incomingChildren)),
                  peon
              )
          );
        }
      };

      zkClient.subscribeChildChanges(serviceName, peonListener);
      exec.execute(
          new UpdatingRunnable(serviceName, currChildren, Sets.newHashSet(zkClient.getChildren(serviceName)), peon)
      );

      listeners.put(serviceName, peon, peonListener);
    }

    @Override
    public void unregisterListener(String serviceName, PhoneBookPeon peon)
    {
      IZkChildListener peonListener = listeners.get(serviceName, peon);

      zkClient.unsubscribeChildChanges(serviceName, peonListener);
    }

    @Override
    public String combineParts(List<String> parts)
    {
      return JOINER.join(parts);
    }

    private String getPath(String parentPath, String child)
    {
      return JOINER.join(parentPath, child);
    }

    private class UpdatingRunnable<T> implements Runnable
    {
      private final String serviceName;
      private final Set<String> currChildren;
      private final PhoneBookPeon<T> peon;
      private final HashSet<String> incomingChildren;

      public UpdatingRunnable(
          String serviceName,
          Set<String> currChildren,
          final HashSet<String> incomingChildren,
          PhoneBookPeon<T> peon
      )
      {
        this.serviceName = serviceName;
        this.currChildren = currChildren;
        this.peon = peon;
        this.incomingChildren = incomingChildren;
      }

      @Override
      public void run()
      {
        try {
          for (String newChild : Sets.difference(incomingChildren, currChildren)) {
            log.debug("  New child[%s], for peon[%s]", newChild, peon);
            String nodeContent;
            try {
              final String data = zkClient.readData(getPath(serviceName, newChild));
              if (data != null) {
                nodeContent = data.toString();
              }
              else {
                log.error("Ignoring path[%s] with null data", getPath(serviceName, newChild));
                continue;
              }
            }
            catch (ZkNoNodeException e) {
              log.info(
                  "Got ZkNoNodeException[%s], node must have gone bye bye before this had a chance to run.",
                  e.getMessage()
              );
              continue;
            }
            T nodeProperties = jsonMapper.readValue(nodeContent, peon.getObjectClazz());

            peon.newEntry(newChild, nodeProperties);
            currChildren.add(newChild);
          }

          // Sets.difference is lazy, so we have to materialize the difference before removing from the sets
          Set<String> setDiff = new HashSet<String>(Sets.difference(currChildren, incomingChildren));
          for (String childRemoved : setDiff) {
            log.debug("  Lost child[%s], for peon[%s]", childRemoved, peon);
            peon.entryRemoved(childRemoved);
            currChildren.remove(childRemoved);
          }
        }
        catch (Exception e) {
          log.warn(e, "Exception thrown, serviceName[%s].", serviceName);
          throw new RuntimeException(e);
        }
      }
    }
  }
}
