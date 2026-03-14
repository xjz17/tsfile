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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public abstract class SubcolumnEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 128;

  private static final int[] DEFAULT_THRESHOLD = {
    2, 3, 5, 8, 9, 11, 14, 16, 17, 17, 18, 19, 20, 21, 22, 22,
    23, 24, 24, 24, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27
  };

  private static final int[] THRESHOLD_64 = {
    2, 3, 5, 9, 13, 17, 19, 24, 29, 32, 33, 33, 35, 37, 39, 40,
    42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 51, 52, 52, 52
  };

  private static final int[] THRESHOLD_128 = {
    2, 3, 5, 9, 17, 22, 33, 33, 43, 52, 59, 64, 65, 65, 69, 72,
    76, 79, 81, 84, 86, 88, 90, 91, 93, 94, 95, 96, 98, 99, 100, 100
  };

  private static final int[] THRESHOLD_256 = {
    2, 3, 5, 9, 17, 33, 37, 64, 65, 77, 94, 107, 119, 128, 129, 129,
    136, 143, 149, 154, 159, 163, 167, 171, 175, 178, 181, 183, 186, 188, 190, 192
  };

  private static final int[] THRESHOLD_512 = {
    2, 3, 5, 9, 17, 33, 65, 65, 114, 129, 140, 171, 197, 220, 239, 256,
    257, 257, 270, 282, 293, 303, 312, 320, 328, 335, 342, 348, 354, 359, 364, 368
  };

  private static final int[] THRESHOLD_1024 = {
    2, 3, 5, 9, 17, 33, 65, 128, 129, 205, 257, 257, 316, 366, 410, 448,
    482, 512, 513, 513, 537, 559, 579, 598, 615, 631, 645, 659, 671, 683, 694, 704
  };

  private static final int[] THRESHOLD_2048 = {
    2, 3, 5, 9, 17, 33, 65, 129, 228, 257, 373, 512, 513, 586, 683, 768,
    844, 911, 971, 1024, 1025, 1025, 1069, 1110, 1147, 1182, 1214, 1244, 1272, 1298, 1322, 1344
  };

  private static final int[] THRESHOLD_4096 = {
    2, 3, 5, 9, 17, 33, 65, 129, 257, 410, 513, 683, 946, 1025, 1093, 1280,
    1446, 1593, 1725, 1844, 1951, 2048, 2049, 2049, 2130, 2206, 2276, 2341, 2402, 2458, 2511, 2560
  };

  private static final int[] THRESHOLD_8192 = {
    2, 3, 5, 9, 17, 33, 65, 129, 257, 513, 745, 1025, 1261, 1756, 2049, 2049,
    2410, 2731, 3019, 3277, 3511, 3724, 3918, 4096, 4097, 4097, 4248, 4389, 4520, 4643, 4757, 4864
  };

  private static final int[] BETA_LIST = {2, 3, 4};

  private static final Logger logger = LoggerFactory.getLogger(SubcolumnEncoder.class);

  protected ByteArrayOutputStream out;
  protected int blockSize;
  protected byte[] encodingBlockBuffer;

  protected int writeIndex = 0;
  protected int writeWidth = 0;

  protected SubcolumnEncoder(int size) {
    super(TSEncoding.SUBCOLUMN);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void writeValueToBytes() throws IOException;

  protected abstract void calcTwoDiff(int i);

  protected abstract void reset();

  protected abstract int calculateBitWidthsForDeltaBlockBuffer();

  protected void ensureCapacity(int minCapacity) {
    if (encodingBlockBuffer.length >= minCapacity) {
      return;
    }
    int newCapacity = Math.max(minCapacity, encodingBlockBuffer.length * 2);
    encodingBlockBuffer = Arrays.copyOf(encodingBlockBuffer, newCapacity);
  }

  private void writeHeaderToBytes() throws IOException {
    ReadWriteIOUtils.write(writeIndex, out);
    ReadWriteIOUtils.write(writeWidth, out);
    writeHeader();
  }

  private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == 0) {
      return;
    }

    this.out = out;
    for (int i = 0; i < writeIndex; i++) {
      calcTwoDiff(i);
    }

    writeWidth = calculateBitWidthsForDeltaBlockBuffer();
    writeHeaderToBytes();
    if (writeWidth != 0) {
      writeValueToBytes();
    }

    reset();
    writeIndex = 0;
    writeWidth = 0;
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  protected static int bytesForBitPacking(int bitWidth, int numValues) {
    if (bitWidth <= 0 || numValues <= 0) {
      return 0;
    }
    return (int) ((((long) bitWidth) * numValues + 7) / 8);
  }

  protected static int getThreshold(int blockSize, int beta) {
    int[] threshold;
    switch (blockSize) {
      case 64:
        threshold = THRESHOLD_64;
        break;
      case 128:
        threshold = THRESHOLD_128;
        break;
      case 256:
        threshold = THRESHOLD_256;
        break;
      case 512:
        threshold = THRESHOLD_512;
        break;
      case 1024:
        threshold = THRESHOLD_1024;
        break;
      case 2048:
        threshold = THRESHOLD_2048;
        break;
      case 4096:
        threshold = THRESHOLD_4096;
        break;
      case 8192:
        threshold = THRESHOLD_8192;
        break;
      case 32:
      default:
        threshold = DEFAULT_THRESHOLD;
        break;
    }
    int index = Math.max(0, Math.min(beta - 1, threshold.length - 1));
    return threshold[index];
  }

  public static void pack8Values(
      int[] values, int offset, int width, int encodePos, byte[] encodedResult) {
    int bufIdx = 0;
    int valueIdx = offset;
    int leftBit = 0;

    while (valueIdx < 8 + offset) {
      int buffer = 0;
      int leftSize = 32;

      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (32 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }

      if (leftSize > 0 && valueIdx < 8 + offset) {
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      for (int j = 0; j < 4; j++) {
        encodedResult[encodePos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
        encodePos++;
        bufIdx++;
        if (bufIdx >= width) {
          return;
        }
      }
    }
  }

  public int bitPacking(
      int[] numbers, int bitWidth, int encodePos, byte[] encodedResult, int numValues) {
    if (bitWidth <= 0 || numValues <= 0) {
      return encodePos;
    }

    int blockNum = numValues / 8;
    int remainder = numValues % 8;

    for (int i = 0; i < blockNum; i++) {
      pack8Values(numbers, i * 8, bitWidth, encodePos, encodedResult);
      encodePos += bitWidth;
    }

    encodePos *= 8;

    for (int i = 0; i < remainder; i++) {
      BytesUtils.intToBytes(numbers[blockNum * 8 + i], encodedResult, encodePos, bitWidth);
      encodePos += bitWidth;
    }

    return (encodePos + 7) / 8;
  }

  public static void pack8Values(
      long[] values, int offset, int width, int encodePos, byte[] encodedResult) {
    int bufIdx = 0;
    int valueIdx = offset;
    int leftBit = 0;

    while (valueIdx < 8 + offset) {
      long buffer = 0;
      int leftSize = 64;

      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (64 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }

      if (leftSize > 0 && valueIdx < 8 + offset) {
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      for (int j = 0; j < 8; j++) {
        encodedResult[encodePos] = (byte) ((buffer >>> ((7 - j) * 8)) & 0xFF);
        encodePos++;
        bufIdx++;
        if (bufIdx >= width) {
          return;
        }
      }
    }
  }

  public int bitPacking(
      long[] numbers, int bitWidth, int encodePos, byte[] encodedResult, int numValues) {
    if (bitWidth <= 0 || numValues <= 0) {
      return encodePos;
    }

    int blockNum = numValues / 8;
    int remainder = numValues % 8;

    for (int i = 0; i < blockNum; i++) {
      pack8Values(numbers, i * 8, bitWidth, encodePos, encodedResult);
      encodePos += bitWidth;
    }

    encodePos *= 8;

    for (int i = 0; i < remainder; i++) {
      BytesUtils.longToBytes(numbers[blockNum * 8 + i], encodedResult, encodePos, bitWidth);
      encodePos += bitWidth;
    }

    return (encodePos + 7) / 8;
  }

  public static class IntSubcolumnEncoder extends SubcolumnEncoder {

    private final int[] deltaBlockBuffer;
    private int minDeltaBase;

    public IntSubcolumnEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    public IntSubcolumnEncoder(int size) {
      super(size);
      deltaBlockBuffer = new int[this.blockSize];
      encodingBlockBuffer = new byte[Math.max(blockSize * Integer.BYTES + 256, 512)];
      reset();
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }

    private void calcDelta(int value) {
      if (value < minDeltaBase) {
        minDeltaBase = value;
      }
      deltaBlockBuffer[writeIndex++] = value;
    }

    public void encodeValue(int value, ByteArrayOutputStream out) {
      calcDelta(value);
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected void reset() {
      minDeltaBase = Integer.MAX_VALUE;
      Arrays.fill(deltaBlockBuffer, 0);
      Arrays.fill(encodingBlockBuffer, (byte) 0);
    }

    private int getValueWidth(int value) {
      return value == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(value);
    }

    private int countGroupedRuns(int shiftAmount, int mask) {
      int previous = (deltaBlockBuffer[0] >>> shiftAmount) & mask;
      int runs = 1;
      for (int i = 1; i < writeIndex; i++) {
        int current = (deltaBlockBuffer[i] >>> shiftAmount) & mask;
        if (current != previous) {
          runs++;
          previous = current;
        }
      }
      return runs;
    }

    private int countDistinctValuesUntilLimit(int shiftAmount, int mask, int limit) {
      int seenMask = 0;
      int distinctCount = 0;
      for (int i = 0; i < writeIndex; i++) {
        int value = (deltaBlockBuffer[i] >>> shiftAmount) & mask;
        int bit = 1 << value;
        if ((seenMask & bit) == 0) {
          seenMask |= bit;
          distinctCount++;
          if (distinctCount >= limit) {
            return distinctCount;
          }
        }
      }
      return distinctCount;
    }

    private int selectBetaAndTypes(int[] encodingType) {
      if (writeWidth == 0) {
        return 1;
      }

      int betaBest = 1;
      int[] bpeCostSingle = new int[writeWidth];
      int[] rleCostSingle = new int[writeWidth];
      int[] deCostSingle = new int[writeWidth];
      int lengthBitWidth = getValueWidth(writeIndex);
      int cost1 = 0;

      for (int i = 0; i < writeWidth; i++) {
        int currentValue = (deltaBlockBuffer[0] >>> i) & 1;
        boolean hasOne = currentValue == 1;
        int runCount = 1;
        boolean changed = false;

        for (int j = 1; j < writeIndex; j++) {
          int subcolumnValue = (deltaBlockBuffer[j] >>> i) & 1;
          if (subcolumnValue == 1) {
            hasOne = true;
          }
          if (subcolumnValue != currentValue) {
            runCount++;
            currentValue = subcolumnValue;
            changed = true;
          }
        }

        bpeCostSingle[i] = hasOne ? writeIndex : 0;
        rleCostSingle[i] = runCount * (1 + lengthBitWidth);
        deCostSingle[i] = changed ? writeIndex * 2 + 2 : writeIndex + 2;

        if (bpeCostSingle[i] <= rleCostSingle[i] && bpeCostSingle[i] <= deCostSingle[i]) {
          encodingType[i] = 0;
          cost1 += bpeCostSingle[i];
        } else if (rleCostSingle[i] < bpeCostSingle[i]
            && rleCostSingle[i] <= deCostSingle[i]) {
          encodingType[i] = 1;
          cost1 += rleCostSingle[i];
        } else {
          encodingType[i] = 2;
          cost1 += deCostSingle[i];
        }
      }

      int minCost = cost1;
      for (int beta : BETA_LIST) {
        if (beta > writeWidth) {
          break;
        }

        int l = (writeWidth + beta - 1) / beta;
        int cost = 0;
        int[] currentEncodingType = new int[l];
        int mask = (1 << beta) - 1;

        for (int i = 0; i < l; i++) {
          int groupStart = i * beta;
          int groupEnd = Math.min(writeWidth, groupStart + beta);
          int betaStart = groupEnd - 1;
          while (betaStart >= groupStart && bpeCostSingle[betaStart] == 0) {
            betaStart--;
          }
          if (betaStart < groupStart) {
            betaStart = groupStart;
          }

          int currentCost = bpeCostSingle[betaStart] * (betaStart - groupStart + 1);

          int rleCostMax = 0;
          for (int j = groupStart; j < groupEnd; j++) {
            rleCostMax = Math.max(rleCostMax, rleCostSingle[j]);
          }

          if (rleCostMax < currentCost) {
            int runCount = countGroupedRuns(groupStart, mask);
            int rleCost = runCount * (beta + lengthBitWidth);
            if (rleCost < currentCost) {
              currentCost = rleCost;
              currentEncodingType[i] = 1;
            }
          }

          int deCostMax = 0;
          for (int j = groupStart; j < groupEnd; j++) {
            deCostMax = Math.max(deCostMax, deCostSingle[j]);
          }

          if (deCostMax < currentCost) {
            int threshold = getThreshold(blockSize, beta);
            int distinctCount = countDistinctValuesUntilLimit(groupStart, mask, threshold);
            if (distinctCount < threshold) {
              int deCost = writeIndex * getValueWidth(distinctCount) + distinctCount * beta;
              if (deCost < currentCost) {
                currentCost = deCost;
                currentEncodingType[i] = 2;
              }
            }
          }

          cost += currentCost;
        }

        if (cost < minCost) {
          minCost = cost;
          betaBest = beta;
          Arrays.fill(encodingType, 0);
          System.arraycopy(currentEncodingType, 0, encodingType, 0, l);
        }
      }

      return betaBest;
    }

    @Override
    protected void writeValueToBytes() throws IOException {
      int[] encodingType = new int[writeWidth];
      int beta = selectBetaAndTypes(encodingType);
      int l = (writeWidth + beta - 1) / beta;
      int[] bitWidthList = new int[l];
      int[] subcolumnBuffer = new int[writeIndex];
      int[] runLength = new int[writeIndex];
      int[] rleValues = new int[writeIndex];
      int mask = (1 << beta) - 1;
      int encodePos = 0;

      ensureCapacity(1);
      encodingBlockBuffer[encodePos++] = (byte) beta;

      for (int i = 0; i < l; i++) {
        int maxValuePart = 0;
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          int current = (deltaBlockBuffer[j] >>> shiftAmount) & mask;
          if (current > maxValuePart) {
            maxValuePart = current;
          }
        }
        bitWidthList[i] = getValueWidth(maxValuePart);
      }

      ensureCapacity(encodePos + bytesForBitPacking(8, l));
      encodePos = bitPacking(bitWidthList, 8, encodePos, encodingBlockBuffer, l);

      int preTypePos = encodePos;
      int encodingTypeBytes = bytesForBitPacking(2, l);
      ensureCapacity(encodePos + encodingTypeBytes);
      encodePos += encodingTypeBytes;

      int runLengthBitWidth = getValueWidth(writeIndex);
      boolean[] seenValues = new boolean[mask + 1];
      int[] dictKeyList = new int[mask + 1];
      int[] codeMap = new int[mask + 1];

      for (int i = 0; i < l; i++) {
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          subcolumnBuffer[j] = (deltaBlockBuffer[j] >>> shiftAmount) & mask;
        }

        int currentBitWidth = bitWidthList[i];
        if (encodingType[i] == 0) {
          ensureCapacity(encodePos + bytesForBitPacking(currentBitWidth, writeIndex));
          encodePos =
              bitPacking(subcolumnBuffer, currentBitWidth, encodePos, encodingBlockBuffer, writeIndex);
          continue;
        }

        if (encodingType[i] == 1) {
          int previous = subcolumnBuffer[0];
          int runCount = 0;
          for (int j = 1; j < writeIndex; j++) {
            int current = subcolumnBuffer[j];
            if (current != previous) {
              runLength[runCount] = j;
              rleValues[runCount] = previous;
              runCount++;
              previous = current;
            }
          }
          runLength[runCount] = writeIndex;
          rleValues[runCount] = previous;
          runCount++;

          ensureCapacity(
              encodePos
                  + 2
                  + bytesForBitPacking(runLengthBitWidth, runCount)
                  + bytesForBitPacking(currentBitWidth, runCount));
          encodingBlockBuffer[encodePos++] = (byte) (runCount >>> 8);
          encodingBlockBuffer[encodePos++] = (byte) runCount;
          encodePos =
              bitPacking(runLength, runLengthBitWidth, encodePos, encodingBlockBuffer, runCount);
          encodePos = bitPacking(rleValues, currentBitWidth, encodePos, encodingBlockBuffer, runCount);
          continue;
        }

        Arrays.fill(seenValues, false);
        int cardinality = 0;
        for (int j = 0; j < writeIndex; j++) {
          int current = subcolumnBuffer[j];
          if (!seenValues[current]) {
            seenValues[current] = true;
            cardinality++;
          }
        }

        int dictBitWidth = getValueWidth(cardinality);
        int dictSize = 0;
        for (int value = 0; value <= mask; value++) {
          if (seenValues[value]) {
            dictKeyList[dictSize] = value;
            codeMap[value] = dictSize;
            dictSize++;
          }
        }

        for (int j = 0; j < writeIndex; j++) {
          subcolumnBuffer[j] = codeMap[subcolumnBuffer[j]];
        }

        ensureCapacity(
            encodePos
                + 2
                + bytesForBitPacking(currentBitWidth, cardinality)
                + bytesForBitPacking(dictBitWidth, writeIndex));
        encodingBlockBuffer[encodePos++] = (byte) (cardinality >>> 8);
        encodingBlockBuffer[encodePos++] = (byte) cardinality;
        encodePos =
            bitPacking(dictKeyList, currentBitWidth, encodePos, encodingBlockBuffer, cardinality);
        encodePos =
            bitPacking(subcolumnBuffer, dictBitWidth, encodePos, encodingBlockBuffer, writeIndex);
      }

      bitPacking(encodingType, 2, preTypePos, encodingBlockBuffer, l);
      ReadWriteIOUtils.write(encodePos, out);
      out.write(encodingBlockBuffer, 0, encodePos);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] -= minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDeltaBase, out);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return Integer.BYTES;
    }

    @Override
    public long getMaxByteSize() {
      return 4L * blockSize + encodingBlockBuffer.length + 8L;
    }
  }

  public static class LongSubcolumnEncoder extends SubcolumnEncoder {

    private final long[] deltaBlockBuffer;
    private long minDeltaBase;

    public LongSubcolumnEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    public LongSubcolumnEncoder(int size) {
      super(size);
      deltaBlockBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[Math.max(blockSize * Long.BYTES + 512, 1024)];
      reset();
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }

    private void calcDelta(long value) {
      if (value < minDeltaBase) {
        minDeltaBase = value;
      }
      deltaBlockBuffer[writeIndex++] = value;
    }

    public void encodeValue(long value, ByteArrayOutputStream out) {
      calcDelta(value);
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected void reset() {
      minDeltaBase = Long.MAX_VALUE;
      Arrays.fill(deltaBlockBuffer, 0L);
      Arrays.fill(encodingBlockBuffer, (byte) 0);
    }

    private int getValueWidth(long value) {
      return value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
    }

    private int countGroupedRuns(int shiftAmount, int mask) {
      int previous = (int) ((deltaBlockBuffer[0] >>> shiftAmount) & mask);
      int runs = 1;
      for (int i = 1; i < writeIndex; i++) {
        int current = (int) ((deltaBlockBuffer[i] >>> shiftAmount) & mask);
        if (current != previous) {
          runs++;
          previous = current;
        }
      }
      return runs;
    }

    private int countDistinctValuesUntilLimit(int shiftAmount, int mask, int limit) {
      int seenMask = 0;
      int distinctCount = 0;
      for (int i = 0; i < writeIndex; i++) {
        int value = (int) ((deltaBlockBuffer[i] >>> shiftAmount) & mask);
        int bit = 1 << value;
        if ((seenMask & bit) == 0) {
          seenMask |= bit;
          distinctCount++;
          if (distinctCount >= limit) {
            return distinctCount;
          }
        }
      }
      return distinctCount;
    }

    private int selectBetaAndTypes(int[] encodingType) {
      if (writeWidth == 0) {
        return 1;
      }

      int betaBest = 1;
      int[] bpeCostSingle = new int[writeWidth];
      int[] rleCostSingle = new int[writeWidth];
      int[] deCostSingle = new int[writeWidth];
      int lengthBitWidth = getValueWidth(writeIndex);
      int cost1 = 0;

      for (int i = 0; i < writeWidth; i++) {
        int currentValue = (int) ((deltaBlockBuffer[0] >>> i) & 1L);
        boolean hasOne = currentValue == 1;
        int runCount = 1;
        boolean changed = false;

        for (int j = 1; j < writeIndex; j++) {
          int subcolumnValue = (int) ((deltaBlockBuffer[j] >>> i) & 1L);
          if (subcolumnValue == 1) {
            hasOne = true;
          }
          if (subcolumnValue != currentValue) {
            runCount++;
            currentValue = subcolumnValue;
            changed = true;
          }
        }

        bpeCostSingle[i] = hasOne ? writeIndex : 0;
        rleCostSingle[i] = runCount * (1 + lengthBitWidth);
        deCostSingle[i] = changed ? writeIndex * 2 + 2 : writeIndex + 2;

        if (bpeCostSingle[i] <= rleCostSingle[i] && bpeCostSingle[i] <= deCostSingle[i]) {
          encodingType[i] = 0;
          cost1 += bpeCostSingle[i];
        } else if (rleCostSingle[i] < bpeCostSingle[i]
            && rleCostSingle[i] <= deCostSingle[i]) {
          encodingType[i] = 1;
          cost1 += rleCostSingle[i];
        } else {
          encodingType[i] = 2;
          cost1 += deCostSingle[i];
        }
      }

      int minCost = cost1;
      for (int beta : BETA_LIST) {
        if (beta > writeWidth) {
          break;
        }

        int l = (writeWidth + beta - 1) / beta;
        int cost = 0;
        int[] currentEncodingType = new int[l];
        int mask = (1 << beta) - 1;

        for (int i = 0; i < l; i++) {
          int groupStart = i * beta;
          int groupEnd = Math.min(writeWidth, groupStart + beta);
          int betaStart = groupEnd - 1;
          while (betaStart >= groupStart && bpeCostSingle[betaStart] == 0) {
            betaStart--;
          }
          if (betaStart < groupStart) {
            betaStart = groupStart;
          }

          int currentCost = bpeCostSingle[betaStart] * (betaStart - groupStart + 1);

          int rleCostMax = 0;
          for (int j = groupStart; j < groupEnd; j++) {
            rleCostMax = Math.max(rleCostMax, rleCostSingle[j]);
          }

          if (rleCostMax < currentCost) {
            int runCount = countGroupedRuns(groupStart, mask);
            int rleCost = runCount * (beta + lengthBitWidth);
            if (rleCost < currentCost) {
              currentCost = rleCost;
              currentEncodingType[i] = 1;
            }
          }

          int deCostMax = 0;
          for (int j = groupStart; j < groupEnd; j++) {
            deCostMax = Math.max(deCostMax, deCostSingle[j]);
          }

          if (deCostMax < currentCost) {
            int threshold = getThreshold(blockSize, beta);
            int distinctCount = countDistinctValuesUntilLimit(groupStart, mask, threshold);
            if (distinctCount < threshold) {
              int deCost = writeIndex * getValueWidth(distinctCount) + distinctCount * beta;
              if (deCost < currentCost) {
                currentCost = deCost;
                currentEncodingType[i] = 2;
              }
            }
          }

          cost += currentCost;
        }

        if (cost < minCost) {
          minCost = cost;
          betaBest = beta;
          Arrays.fill(encodingType, 0);
          System.arraycopy(currentEncodingType, 0, encodingType, 0, l);
        }
      }

      return betaBest;
    }

    @Override
    protected void writeValueToBytes() throws IOException {
      int[] encodingType = new int[writeWidth];
      int beta = selectBetaAndTypes(encodingType);
      int l = (writeWidth + beta - 1) / beta;
      int[] bitWidthList = new int[l];
      long[] subcolumnBuffer = new long[writeIndex];
      int[] runLength = new int[writeIndex];
      long[] rleValues = new long[writeIndex];
      int mask = (1 << beta) - 1;
      int encodePos = 0;

      ensureCapacity(1);
      encodingBlockBuffer[encodePos++] = (byte) beta;

      for (int i = 0; i < l; i++) {
        long maxValuePart = 0;
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          long current = (deltaBlockBuffer[j] >>> shiftAmount) & mask;
          if (current > maxValuePart) {
            maxValuePart = current;
          }
        }
        bitWidthList[i] = getValueWidth(maxValuePart);
      }

      ensureCapacity(encodePos + bytesForBitPacking(8, l));
      encodePos = bitPacking(bitWidthList, 8, encodePos, encodingBlockBuffer, l);

      int preTypePos = encodePos;
      int encodingTypeBytes = bytesForBitPacking(2, l);
      ensureCapacity(encodePos + encodingTypeBytes);
      encodePos += encodingTypeBytes;

      int runLengthBitWidth = getValueWidth(writeIndex);
      boolean[] seenValues = new boolean[mask + 1];
      long[] dictKeyList = new long[mask + 1];
      int[] codeMap = new int[mask + 1];

      for (int i = 0; i < l; i++) {
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          subcolumnBuffer[j] = (deltaBlockBuffer[j] >>> shiftAmount) & mask;
        }

        int currentBitWidth = bitWidthList[i];
        if (encodingType[i] == 0) {
          ensureCapacity(encodePos + bytesForBitPacking(currentBitWidth, writeIndex));
          encodePos =
              bitPacking(subcolumnBuffer, currentBitWidth, encodePos, encodingBlockBuffer, writeIndex);
          continue;
        }

        if (encodingType[i] == 1) {
          long previous = subcolumnBuffer[0];
          int runCount = 0;
          for (int j = 1; j < writeIndex; j++) {
            long current = subcolumnBuffer[j];
            if (current != previous) {
              runLength[runCount] = j;
              rleValues[runCount] = previous;
              runCount++;
              previous = current;
            }
          }
          runLength[runCount] = writeIndex;
          rleValues[runCount] = previous;
          runCount++;

          ensureCapacity(
              encodePos
                  + 2
                  + bytesForBitPacking(runLengthBitWidth, runCount)
                  + bytesForBitPacking(currentBitWidth, runCount));
          encodingBlockBuffer[encodePos++] = (byte) (runCount >>> 8);
          encodingBlockBuffer[encodePos++] = (byte) runCount;
          encodePos =
              bitPacking(runLength, runLengthBitWidth, encodePos, encodingBlockBuffer, runCount);
          encodePos = bitPacking(rleValues, currentBitWidth, encodePos, encodingBlockBuffer, runCount);
          continue;
        }

        Arrays.fill(seenValues, false);
        int cardinality = 0;
        for (int j = 0; j < writeIndex; j++) {
          int current = (int) subcolumnBuffer[j];
          if (!seenValues[current]) {
            seenValues[current] = true;
            cardinality++;
          }
        }

        int dictBitWidth = getValueWidth(cardinality);
        int dictSize = 0;
        for (int value = 0; value <= mask; value++) {
          if (seenValues[value]) {
            dictKeyList[dictSize] = value;
            codeMap[value] = dictSize;
            dictSize++;
          }
        }

        for (int j = 0; j < writeIndex; j++) {
          subcolumnBuffer[j] = codeMap[(int) subcolumnBuffer[j]];
        }

        ensureCapacity(
            encodePos
                + 2
                + bytesForBitPacking(currentBitWidth, cardinality)
                + bytesForBitPacking(dictBitWidth, writeIndex));
        encodingBlockBuffer[encodePos++] = (byte) (cardinality >>> 8);
        encodingBlockBuffer[encodePos++] = (byte) cardinality;
        encodePos =
            bitPacking(dictKeyList, currentBitWidth, encodePos, encodingBlockBuffer, cardinality);
        encodePos =
            bitPacking(subcolumnBuffer, dictBitWidth, encodePos, encodingBlockBuffer, writeIndex);
      }

      bitPacking(encodingType, 2, preTypePos, encodingBlockBuffer, l);
      ReadWriteIOUtils.write(encodePos, out);
      out.write(encodingBlockBuffer, 0, encodePos);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] -= minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDeltaBase, out);
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return Long.BYTES;
    }

    @Override
    public long getMaxByteSize() {
      return 8L * blockSize + encodingBlockBuffer.length + 12L;
    }
  }
}
