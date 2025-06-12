package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class SubcolumnEncoder extends Encoder {

  protected static final int BLOCK_DEFAULT_SIZE = 128;
  private static final Logger logger = LoggerFactory.getLogger(SubcolumnEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;
  protected byte[] encodingBlockBuffer;

  protected int writeIndex = 0;
  protected int writeWidth = 0;

  protected int encode_pos = 0;
  protected int beta = 3;

  protected SubcolumnEncoder(int size) {
    super(TSEncoding.SUBCOLUMN);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void writeValueToBytes() throws IOException;

  protected abstract void calcTwoDiff(int i);

  protected abstract void reset();

  protected abstract int calculateBitWidthsForDeltaBlockBuffer();

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
    encode_pos = 0;
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  public static void pack8Values(
      int[] values, int offset, int width, int encode_pos, byte[] encoded_result) {
    int bufIdx = 0;
    int valueIdx = offset;
    // remaining bits for the current unfinished Integer
    int leftBit = 0;

    while (valueIdx < 8 + offset) {
      // buffer is used for saving 32 bits as a part of result
      int buffer = 0;
      // remaining size of bits in the 'buffer'
      int leftSize = 32;

      // encode the left bits of current Integer to 'buffer'
      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (32 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        // encode one Integer to the 'buffer'
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Integer,
      if (leftSize > 0 && valueIdx < 8 + offset) {
        // put the first 'leftSize' bits of the Integer into remaining space of the
        // buffer
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      // put the buffer into the final result
      for (int j = 0; j < 4; j++) {
        encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
        encode_pos++;
        bufIdx++;
        if (bufIdx >= width) {
          return;
        }
      }
    }
  }

  public int bitPacking(
      int[] numbers, int bit_width, int encode_pos, byte[] encoded_result, int num_values) {
    int block_num = num_values / 8;
    int remainder = num_values % 8;

    for (int i = 0; i < block_num; i++) {
      pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
      encode_pos += bit_width;
    }

    encode_pos *= 8;

    for (int i = 0; i < remainder; i++) {
      BytesUtils.intToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
      encode_pos += bit_width;
    }

    return (encode_pos + 7) / 8;
  }

  public static void pack8Values(
      long[] values, int offset, int width, int encode_pos, byte[] encoded_result) {
    int bufIdx = 0;
    int valueIdx = offset;
    // remaining bits for the current unfinished Long
    int leftBit = 0;

    while (valueIdx < 8 + offset) {
      // buffer is used for saving 64 bits as a part of result
      long buffer = 0;
      // remaining size of bits in the 'buffer'
      int leftSize = 64;

      // encode the left bits of current Long to 'buffer'
      if (leftBit > 0) {
        buffer |= (values[valueIdx] << (64 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        // encode one Long to the 'buffer'
        buffer |= (values[valueIdx] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Long
      if (leftSize > 0 && valueIdx < 8 + offset) {
        // put the first 'leftSize' bits of the Long into remaining space of the buffer
        buffer |= (values[valueIdx] >>> (width - leftSize));
        leftBit = width - leftSize;
      }

      // put the buffer into the final result
      for (int j = 0; j < 8; j++) {
        encoded_result[encode_pos] = (byte) ((buffer >>> ((8 - j - 1) * 8)) & 0xFF);
        encode_pos++;
        bufIdx++;
        if (bufIdx >= width * 8 / 8) {
          return;
        }
      }
    }
  }

  public int bitPacking(
      long[] numbers, int bit_width, int encode_pos, byte[] encoded_result, int num_values) {
    int block_num = num_values / 8;
    int remainder = num_values % 8;

    for (int i = 0; i < block_num; i++) {
      pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
      encode_pos += bit_width;
    }

    encode_pos *= 8;

    for (int i = 0; i < remainder; i++) {
      BytesUtils.longToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
      encode_pos += bit_width;
    }

    return (encode_pos + 7) / 8;
  }

  public static class IntSubcolumnEncoder extends SubcolumnEncoder {

    private int[] deltaBlockBuffer;

    private int minDeltaBase;

    public IntSubcolumnEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    public IntSubcolumnEncoder(int size) {
      super(size);
      deltaBlockBuffer = new int[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 4];
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
      deltaBlockBuffer[writeIndex] = value;
      writeIndex++;
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
      for (int i = 0; i < blockSize; i++) {
        deltaBlockBuffer[i] = 0;
      }
      for (int i = 0; i < blockSize * 4; i++) {
        encodingBlockBuffer[i] = 0;
      }
    }

    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes() throws IOException {

      int l = (writeWidth + beta - 1) / beta;

      int[] bitWidthList = new int[l];

      int[][] subcolumnList = new int[l][writeIndex];

      int bw = getValueWidth(writeIndex);

      int mask = (1 << beta) - 1;

      for (int i = 0; i < l; i++) {
        int maxValuePart = 0;
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          subcolumnList[i][j] = (deltaBlockBuffer[j] >> shiftAmount) & mask;
          if (subcolumnList[i][j] > maxValuePart) {
            maxValuePart = subcolumnList[i][j];
          }
        }
        bitWidthList[i] = getValueWidth(maxValuePart);
      }

      encode_pos = bitPacking(bitWidthList, 8, encode_pos, encodingBlockBuffer, l);

      int[] encodingType = new int[l];

      int preTypePos = encode_pos;
      encode_pos += (l + 7) / 8;

      for (int i = l - 1; i >= 0; i--) {
        int bpCost = bitWidthList[i] * writeIndex;
        int rleCost = 0;

        int previous = subcolumnList[i][0];
        int index = 0;

        for (int j = 1; j < writeIndex; j++) {
          int currentNumber = subcolumnList[i][j];
          if (currentNumber != previous) {
            index++;
            previous = currentNumber;
          }

          if (bw * index + bitWidthList[i] * index >= bpCost) {
            break;
          }
        }

        index++;

        rleCost = bw * index + bitWidthList[i] * index;

        if (bpCost <= rleCost) {
          encodingType[i] = 0;

          encode_pos =
              bitPacking(
                  subcolumnList[i], bitWidthList[i], encode_pos, encodingBlockBuffer, writeIndex);

        } else {
          encodingType[i] = 1;

          encodingBlockBuffer[encode_pos] = (byte) (index >> 8);
          encode_pos += 1;
          encodingBlockBuffer[encode_pos] = (byte) (index & 0xFF);
          encode_pos += 1;

          index = 0;
          int[] run_length = new int[writeIndex];
          int[] rle_values = new int[writeIndex];
          previous = subcolumnList[i][0];

          for (int j = 1; j < writeIndex; j++) {
            int currentNumber = subcolumnList[i][j];
            if (currentNumber != previous) {
              run_length[index] = j;
              rle_values[index] = previous;
              index++;
              previous = currentNumber;
            }
          }

          run_length[index] = writeIndex;
          rle_values[index] = previous;
          index++;

          encode_pos = bitPacking(run_length, bw, encode_pos, encodingBlockBuffer, index);

          encode_pos =
              bitPacking(rle_values, bitWidthList[i], encode_pos, encodingBlockBuffer, index);
        }
      }

      preTypePos = bitPacking(encodingType, 1, preTypePos, encodingBlockBuffer, l);

      ReadWriteIOUtils.write(encode_pos, out);

      out.write(encodingBlockBuffer, 0, encode_pos);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(minDeltaBase, out);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }
  }

  public static class LongSubcolumnEncoder extends SubcolumnEncoder {

    private long[] deltaBlockBuffer;

    private long minDeltaBase;

    public LongSubcolumnEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    public LongSubcolumnEncoder(int size) {
      super(size);
      deltaBlockBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 8];
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
      deltaBlockBuffer[writeIndex] = value;
      writeIndex++;
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
      for (int i = 0; i < blockSize; i++) {
        deltaBlockBuffer[i] = 0;
      }
      for (int i = 0; i < blockSize * 8; i++) {
        encodingBlockBuffer[i] = 0;
      }
    }

    private int getValueWidth(long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes() throws IOException {

      int l = (writeWidth + beta - 1) / beta;

      int[] bitWidthList = new int[l];

      long[][] subcolumnList = new long[l][writeIndex];

      int bw = getValueWidth(writeIndex);

      int mask = (1 << beta) - 1;

      for (int i = 0; i < l; i++) {
        long maxValuePart = 0;
        int shiftAmount = i * beta;
        for (int j = 0; j < writeIndex; j++) {
          subcolumnList[i][j] = (deltaBlockBuffer[j] >> shiftAmount) & mask;
          if (subcolumnList[i][j] > maxValuePart) {
            maxValuePart = subcolumnList[i][j];
          }
        }
        bitWidthList[i] = getValueWidth(maxValuePart);
      }

      encode_pos = bitPacking(bitWidthList, 8, encode_pos, encodingBlockBuffer, l);

      int[] encodingType = new int[l];

      int preTypePos = encode_pos;
      encode_pos += (l + 7) / 8;

      for (int i = l - 1; i >= 0; i--) {
        long bpCost = bitWidthList[i] * writeIndex;
        long rleCost = 0;

        long previous = subcolumnList[i][0];
        int index = 0;

        for (int j = 1; j < writeIndex; j++) {
          long currentNumber = subcolumnList[i][j];
          if (currentNumber != previous) {
            index++;
            previous = currentNumber;
          }

          if (bw * index + bitWidthList[i] * index >= bpCost) {
            break;
          }
        }

        index++;

        rleCost = bw * index + bitWidthList[i] * index;

        if (bpCost <= rleCost) {
          encodingType[i] = 0;

          encode_pos =
              bitPacking(
                  subcolumnList[i], bitWidthList[i], encode_pos, encodingBlockBuffer, writeIndex);

        } else {
          encodingType[i] = 1;

          encodingBlockBuffer[encode_pos] = (byte) (index >> 8);
          encode_pos += 1;
          encodingBlockBuffer[encode_pos] = (byte) (index & 0xFF);
          encode_pos += 1;

          index = 0;
          long[] run_length = new long[writeIndex];
          long[] rle_values = new long[writeIndex];
          previous = subcolumnList[i][0];

          for (int j = 1; j < writeIndex; j++) {
            long currentNumber = subcolumnList[i][j];
            if (currentNumber != previous) {
              run_length[index] = j;
              rle_values[index] = previous;
              index++;
              previous = currentNumber;
            }
          }

          run_length[index] = writeIndex;
          rle_values[index] = previous;
          index++;

          encode_pos = bitPacking(run_length, bw, encode_pos, encodingBlockBuffer, index);

          encode_pos =
              bitPacking(rle_values, bitWidthList[i], encode_pos, encodingBlockBuffer, index);
        }
      }

      preTypePos = bitPacking(encodingType, 1, preTypePos, encodingBlockBuffer, l);

      ReadWriteIOUtils.write(encode_pos, out);

      out.write(encodingBlockBuffer, 0, encode_pos);
    }

    @Override
    protected void calcTwoDiff(int i) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaBase;
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.longToBytes(minDeltaBase));
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }
  }
}
