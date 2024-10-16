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

export interface Highlight {
  /**
   * The label of the highlight.
   */
  label: string;

  /**
   * The x coordinate of the highlight.
   */
  x: number;

  /**
   * The y coordinate of the highlight.
   */
  y: number;

  /**
   * Optional x offset for the highlight (useful for scrolling offset)
   */
  offsetX?: number;

  /**
   * Optional y offset for the highlight (useful for scrolling offset)
   */
  offsetY?: number;

  /**
   * Called when the highlight is dropped (when the "cancel" button is clicked)
   */
  onDrop: () => void;

  /**
   * Called when the highlight is saved (when the "save" button is clicked)
   * @param highlight The highlight to save
   */
  onSave?: (highlight: Highlight) => void;

  /**
   * Optional data attached to the highlight.
   */
  data?: any;
}
