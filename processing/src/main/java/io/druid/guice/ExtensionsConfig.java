/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class ExtensionsConfig
{
  @JsonProperty
  @NotNull
  private boolean searchCurrentClassloader = true;

  @JsonProperty
  @NotNull
  private List<String> coordinates = ImmutableList.of();

  @JsonProperty
  @NotNull
  private String localRepository = String.format("%s/%s", System.getProperty("user.home"), ".m2/repository");

  @JsonProperty
  @NotNull
  private List<String> remoteRepositories = ImmutableList.of(
      "http://repo1.maven.org/maven2/",
      "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local"
  );

  public boolean searchCurrentClassloader()
  {
    return searchCurrentClassloader;
  }

  public List<String> getCoordinates()
  {
    return coordinates;
  }

  public String getLocalRepository()
  {
    return localRepository;
  }

  public List<String> getRemoteRepositories()
  {
    return remoteRepositories;
  }

  @Override
  public String toString()
  {
    return "ExtensionsConfig{" +
           "searchCurrentClassloader=" + searchCurrentClassloader +
           ", coordinates=" + coordinates +
           ", localRepository='" + localRepository + '\'' +
           ", remoteRepositories=" + remoteRepositories +
           '}';
  }
}
