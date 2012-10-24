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

/**
 * A PhoneBookPeon is a Dilbert-like character who sits in his cubicle all day just waiting to hear about newEntry()s
 * and removed entries.  He acts as the go between, someone that the PhoneBook knows how to talk to and can then
 * translate said message into whatever his employer wants.
 */
public interface PhoneBookPeon<T>
{
  public Class<T> getObjectClazz();
  public void newEntry(String name, T properties);
  public void entryRemoved(String name);
}
