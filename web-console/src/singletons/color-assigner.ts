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

import { hashJoaat } from '../utils';

export interface ColorPalette {
  nullColor: string;
  categorical: string[];
}

export class ColorAssigner {
  static PALETTE: ColorPalette = {
    nullColor: '#303030',
    categorical: ['#00BD84', '#F78228', '#8A81F3', '#EB57A3', '#7CCE21', '#FFC213', '#B97800'],
  };

  static byDimension = new Map<string, ColorAssigner>();

  static getColorForDimensionValue(dimensionName: string, value: any): string {
    // Find or create a palette
    let dimensionPalette = ColorAssigner.byDimension.get(dimensionName);
    if (!dimensionPalette) {
      dimensionPalette = new ColorAssigner();
      ColorAssigner.byDimension.set(dimensionName, dimensionPalette);
    }
    return dimensionPalette.getColorFor(value);
  }

  private readonly assignments = new Map<number, string>(); // ValueHash -> Color

  private constructor() {}

  private countAssignmentsByColor(): Map<string, number> {
    const { assignments } = this;
    const colorCounts = new Map<string, number>();
    for (const [_, color] of assignments) {
      colorCounts.set(color, (colorCounts.get(color) || 0) + 1);
    }
    return colorCounts;
  }

  getColorFor(thing: any): string {
    // Special colors
    if (thing === null) return ColorAssigner.PALETTE.nullColor;
    if (thing === true) return ColorAssigner.PALETTE.categorical[0];
    if (thing === false) return ColorAssigner.PALETTE.categorical[1];

    const thingKey = String(thing);
    return this.getOrAssign(thingKey);
  }

  private getOrAssign(thingKey: string): string {
    const { assignments } = this;
    const key = hashJoaat(thingKey);
    const existing = assignments.get(key);
    if (existing) return existing;

    const leastUsedColor = this.getLeastUsedColor(key);
    assignments.set(key, leastUsedColor);
    return leastUsedColor;
  }

  private getLeastUsedColor(key: number): string {
    const curAssignments = this.countAssignmentsByColor();
    const { categorical } = ColorAssigner.PALETTE;

    let leastUsedColor = categorical[key % categorical.length];
    let leastUsedCount = curAssignments.get(leastUsedColor) || 0;
    for (const color of categorical) {
      const count = curAssignments.get(color) || 0;
      if (count < leastUsedCount) {
        leastUsedColor = color;
        leastUsedCount = count;
      }
    }
    return leastUsedColor;
  }
}
