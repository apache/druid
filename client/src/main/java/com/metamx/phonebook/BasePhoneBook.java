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

package com.metamx.phonebook;

import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;

import java.util.List;
import java.util.Map;

/**
 */
public class BasePhoneBook implements PhoneBook
{
  private final Object lock = new Object();

  private final StoppedPhoneBook stoppedPages = new StoppedPhoneBook();
  private final PhoneBook actualPages;

  private volatile boolean started = false;

  public BasePhoneBook(PhoneBook actualPages)
  {
    this.actualPages = actualPages;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      actualPages.start();

      for (Map.Entry<String, Map<String, Object>> services : stoppedPages.getAnnouncements().entrySet()) {
        String serviceName = services.getKey();
        for (Map.Entry<String, Object> announcements : services.getValue().entrySet()) {
          String nodeName = announcements.getKey();

          actualPages.announce(serviceName, nodeName, announcements.getValue());
        }
      }

      for (Map.Entry<String, PhoneBookPeon> listenerEntry : stoppedPages.getListeners().entries()) {
        actualPages.registerListener(listenerEntry.getKey(), listenerEntry.getValue());
      }

      started = true;
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      started = false;

      for (Map.Entry<String, Map<String, Object>> services : stoppedPages.getAnnouncements().entrySet()) {
        String serviceName = services.getKey();
        for (String nodeName : services.getValue().keySet()) {
          actualPages.unannounce(serviceName, nodeName);
        }
      }

      for (Map.Entry<String, PhoneBookPeon> listenerEntry : stoppedPages.getListeners().entries()) {
        actualPages.unregisterListener(listenerEntry.getKey(), listenerEntry.getValue());
      }

      actualPages.stop();
    }
  }

  @Override
  public boolean isStarted()
  {
    return started;
  }

  @Override
  public <T> void announce(String serviceName, String nodeName, T properties)
  {
    synchronized (lock) {
      stoppedPages.announce(serviceName, nodeName, properties);

      if (started) {
        actualPages.announce(serviceName, nodeName, properties);
      }
    }
  }

  @Override
  public void unannounce(String serviceName, String nodeName)
  {
    synchronized (lock) {
      stoppedPages.unannounce(serviceName, nodeName);

      if (started) {
        actualPages.unannounce(serviceName, nodeName);
      }
    }
  }

  @Override
  public <T> T lookup(String serviceName, Class<? extends T> clazz)
  {
    synchronized (lock) {
      if (! started) {
        throw  new ISE("Cannot lookup on a stopped PhoneBook.");
      }

      return actualPages.lookup(serviceName, clazz);
    }
  }

  @Override
  public <T> void post(String serviceName, String nodeName, T properties)
  {
    synchronized (lock) {
      if (! started) {
        throw new ISE("Cannot post to a stopped PhoneBook.");
      }

      actualPages.post(serviceName, nodeName, properties);
    }
  }

  @Override
  public boolean unpost(String serviceName, String nodeName)
  {
    synchronized (lock) {
      if (! started) {
        throw new ISE("Cannot post to a stopped PhoneBook.");
      }

      return actualPages.unpost(serviceName, nodeName);
    }
  }

  @Override
  public <T> void postEphemeral(String serviceName, String nodeName, T properties)
  {
    synchronized (lock) {
      if (! started) {
        throw new ISE("Cannot post to a stopped PhoneBook.");
      }

      actualPages.postEphemeral(serviceName, nodeName, properties);
    }
  }

  @Override
  public <T> void registerListener(String serviceName, PhoneBookPeon<T> peon)
  {
    synchronized (lock) {
      stoppedPages.registerListener(serviceName, peon);

      if (started) {
        actualPages.registerListener(serviceName, peon);
      }
    }
  }

  @Override
  public <T> void unregisterListener(String serviceName, PhoneBookPeon<T> peon)
  {
    synchronized (lock) {
      stoppedPages.unregisterListener(serviceName, peon);

      if (started) {
        actualPages.unregisterListener(serviceName, peon);
      }
    }
  }

  @Override
  public String combineParts(List<String> parts)
  {
    return actualPages.combineParts(parts);
  }
}
