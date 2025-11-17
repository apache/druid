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

import { ColorAssigner } from './color-assigner';

describe('ColorAssigner', () => {
  beforeEach(() => {
    ColorAssigner.byDimension.clear();
  });

  describe('getColorForDimensionValue', () => {
    it('should create dimension-specific color assigners', () => {
      const color1 = ColorAssigner.getColorForDimensionValue('dimension1', 'value1');
      const color2 = ColorAssigner.getColorForDimensionValue('dimension2', 'value1');

      expect(color1).toBeDefined();
      expect(color2).toBeDefined();
      expect(ColorAssigner.byDimension.size).toBe(2);
    });

    it('should return consistent colors for same dimension and value', () => {
      const color1 = ColorAssigner.getColorForDimensionValue('dimension1', 'value1');
      const color2 = ColorAssigner.getColorForDimensionValue('dimension1', 'value1');

      expect(color1).toBe(color2);
    });

    it('should use same color assigner instance for same dimension', () => {
      ColorAssigner.getColorForDimensionValue('dimension1', 'value1');
      ColorAssigner.getColorForDimensionValue('dimension1', 'value2');

      expect(ColorAssigner.byDimension.size).toBe(1);
    });
  });

  describe('getColorFor', () => {
    let colorAssigner: any;

    beforeEach(() => {
      colorAssigner = ColorAssigner.getColorForDimensionValue('test', 'dummy');
      ColorAssigner.byDimension.clear();
      colorAssigner = new (ColorAssigner as any)();
    });

    it('should return special color for null', () => {
      expect(colorAssigner.getColorFor(null)).toBe('#303030');
    });

    it('should return first categorical color for true', () => {
      expect(colorAssigner.getColorFor(true)).toBe('#00BD84');
    });

    it('should return second categorical color for false', () => {
      expect(colorAssigner.getColorFor(false)).toBe('#F78228');
    });

    it('should assign colors to string values', () => {
      const color1 = colorAssigner.getColorFor('apple');
      const color2 = colorAssigner.getColorFor('banana');

      expect(ColorAssigner.PALETTE.categorical).toContain(color1);
      expect(ColorAssigner.PALETTE.categorical).toContain(color2);
    });

    it('should return consistent colors for same value', () => {
      const color1 = colorAssigner.getColorFor('apple');
      const color2 = colorAssigner.getColorFor('apple');

      expect(color1).toBe(color2);
    });

    it('should handle numeric values', () => {
      const color1 = colorAssigner.getColorFor(42);
      const color2 = colorAssigner.getColorFor(42);

      expect(color1).toBe(color2);
      expect(ColorAssigner.PALETTE.categorical).toContain(color1);
    });

    it('should convert values to strings before hashing', () => {
      const color1 = colorAssigner.getColorFor(123);
      const color2 = colorAssigner.getColorFor('123');

      expect(color1).toBe(color2);
    });
  });

  describe('color distribution', () => {
    let colorAssigner: any;

    beforeEach(() => {
      colorAssigner = ColorAssigner.getColorForDimensionValue('test', 'dummy');
      ColorAssigner.byDimension.clear();
      colorAssigner = new (ColorAssigner as any)();
    });

    it('should distribute colors evenly', () => {
      const colorCounts = new Map<string, number>();
      const numberOfValues = ColorAssigner.PALETTE.categorical.length * 3;

      for (let i = 0; i < numberOfValues; i++) {
        const color = colorAssigner.getColorFor(`value${i}`);
        colorCounts.set(color, (colorCounts.get(color) || 0) + 1);
      }

      const counts = Array.from(colorCounts.values());
      const minCount = Math.min(...counts);
      const maxCount = Math.max(...counts);

      expect(maxCount - minCount).toBeLessThanOrEqual(1);
    });

    it('should use all available colors', () => {
      const usedColors = new Set<string>();
      const numberOfValues = ColorAssigner.PALETTE.categorical.length;

      for (let i = 0; i < numberOfValues; i++) {
        const color = colorAssigner.getColorFor(`value${i}`);
        usedColors.add(color);
      }

      expect(usedColors.size).toBe(ColorAssigner.PALETTE.categorical.length);
    });
  });

  describe('getLeastUsedColor', () => {
    const colorAssigner = new (ColorAssigner as any)();

    it('should return first color when no assignments exist', () => {
      const leastUsedColor = colorAssigner.getLeastUsedColor(
        ColorAssigner.PALETTE.categorical.length,
      );
      expect(leastUsedColor).toBe(ColorAssigner.PALETTE.categorical[0]);
    });

    it('should return least used color after assignments', () => {
      colorAssigner.getColorFor('value1');
      colorAssigner.getColorFor('value2');

      const leastUsedColor = colorAssigner.getLeastUsedColor(4);
      expect(leastUsedColor).toBe(ColorAssigner.PALETTE.categorical[4]);
    });
  });

  describe('hash collision handling', () => {
    const colorAssigner = new (ColorAssigner as any)();

    it('should handle potential hash collisions gracefully', () => {
      const values = [];
      for (let i = 0; i < 1000; i++) {
        values.push(`value_${i}_${Math.random()}`);
      }

      const colors = values.map(v => colorAssigner.getColorFor(v));
      const uniqueColors = new Set(colors);

      expect(uniqueColors.size).toBeLessThanOrEqual(ColorAssigner.PALETTE.categorical.length);

      for (let i = 0; i < values.length; i++) {
        const consistentColor = colorAssigner.getColorFor(values[i]);
        expect(consistentColor).toBe(colors[i]);
      }
    });
  });
});
