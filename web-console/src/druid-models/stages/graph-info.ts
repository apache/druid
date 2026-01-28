/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { filterMap } from '../../utils';

import type { StageDefinition } from './stages';

export type GraphInfoLane =
  | { type: 'stage'; stageNumber: number; fromLanes: number[]; hasOut: boolean }
  | { type: 'line'; stageNumber: number; fromLane: number };

export type GraphInfo = GraphInfoLane[];

export function computeNextInfo(
  stage: StageDefinition,
  prevInfo: GraphInfo,
  isLastStage: boolean,
): GraphInfo {
  const inputStageNumbers = filterMap(stage.definition.input, i =>
    i.type === 'stage' ? i.stage : undefined,
  );

  const myInfo: GraphInfo = [];
  let addedSelf = false;
  for (let i = 0; i < prevInfo.length; i++) {
    const prevLane = prevInfo[i];
    if (addedSelf) {
      if (inputStageNumbers.includes(prevLane.stageNumber)) {
        // Do nothing because this was already recorded in the 'stage' lane
      } else {
        myInfo.push({ type: 'line', stageNumber: prevLane.stageNumber, fromLane: i });
      }
    } else {
      if (inputStageNumbers.includes(prevLane.stageNumber)) {
        // Add self
        myInfo.push({
          type: 'stage',
          stageNumber: stage.stageNumber,
          fromLanes: filterMap(prevInfo, (lane, i) =>
            inputStageNumbers.includes(lane.stageNumber) ? i : undefined,
          ),
          hasOut: !isLastStage,
        });
        addedSelf = true;
      } else {
        myInfo.push({ type: 'line', stageNumber: prevLane.stageNumber, fromLane: i });
      }
    }
  }

  // If the stage was not already added then it must be a new source, start a new lane
  if (!addedSelf) {
    myInfo.push({
      type: 'stage',
      stageNumber: stage.stageNumber,
      fromLanes: [], // We know the stage is a source
      hasOut: !isLastStage,
    });
  }

  return myInfo;
}
