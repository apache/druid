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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexing.common.task.Task;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "lockAcquire", value = LockAcquireAction.class),
    @JsonSubTypes.Type(name = "lockTryAcquire", value = LockTryAcquireAction.class),
    @JsonSubTypes.Type(name = "lockList", value = LockListAction.class),
    @JsonSubTypes.Type(name = "lockRelease", value = LockReleaseAction.class),
    @JsonSubTypes.Type(name = "segmentInsertion", value = SegmentInsertAction.class),
    @JsonSubTypes.Type(name = "segmentListUsed", value = SegmentListUsedAction.class),
    @JsonSubTypes.Type(name = "segmentListUnused", value = SegmentListUnusedAction.class),
    @JsonSubTypes.Type(name = "segmentNuke", value = SegmentNukeAction.class),
    @JsonSubTypes.Type(name = "segmentMove", value = SegmentMoveAction.class)
})
public interface TaskAction<RetType>
{
  public TypeReference<RetType> getReturnTypeReference(); // T_T
  public RetType perform(Task task, TaskActionToolbox toolbox) throws IOException;
  public boolean isAudited();
}
