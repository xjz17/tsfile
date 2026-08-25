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

package org.apache.tsfile.encoding.common;

import org.apache.tsfile.enums.TSDataType;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;

/** Type-preserving wrapper around {@link ExperimentalLongCodec}. */
public final class ExperimentalNumericPageCodec {

  private static final int VERSION = 1;
  private static final int DIRECT_INT32 = 0;
  private static final int DIRECT_INT64 = 1;
  private static final int RAW_FLOAT = 2;
  private static final int SCALED_FLOAT = 3;
  private static final int RAW_DOUBLE = 4;
  private static final int SCALED_DOUBLE = 5;

  private ExperimentalNumericPageCodec() {}

  public static byte[] encode(
      ExperimentalLongCodec.Kind kind, TSDataType dataType, long[] source, int length) {
    long[] values = Arrays.copyOf(source, length);
    int representation;
    int scale = 0;
    switch (dataType) {
      case INT32:
      case DATE:
        representation = DIRECT_INT32;
        break;
      case INT64:
      case TIMESTAMP:
        representation = DIRECT_INT64;
        break;
      case FLOAT:
        ScaledValues scaledFloats = scaleFloatsExactly(values);
        if (scaledFloats == null) {
          representation = RAW_FLOAT;
        } else {
          representation = SCALED_FLOAT;
          scale = scaledFloats.scale;
          values = scaledFloats.values;
        }
        break;
      case DOUBLE:
        ScaledValues scaledDoubles = scaleDoublesExactly(values);
        if (scaledDoubles == null) {
          representation = RAW_DOUBLE;
        } else {
          representation = SCALED_DOUBLE;
          scale = scaledDoubles.scale;
          values = scaledDoubles.values;
        }
        break;
      default:
        throw new IllegalArgumentException("unsupported experimental codec type: " + dataType);
    }

    byte[] body = ExperimentalLongCodec.encode(kind, values);
    ByteArrayOutputStream out = new ByteArrayOutputStream(body.length + 8);
    out.write(VERSION);
    out.write(representation);
    out.write(scale);
    putInt(out, body.length);
    out.write(body, 0, body.length);
    return out.toByteArray();
  }

  public static long[] decode(
      ExperimentalLongCodec.Kind kind, TSDataType dataType, byte[] payload) {
    Reader reader = new Reader(payload);
    if (reader.readUnsignedByte() != VERSION) {
      throw new IllegalArgumentException("unsupported experimental codec version");
    }
    int representation = reader.readUnsignedByte();
    int scale = reader.readUnsignedByte();
    byte[] body = reader.readBytes(reader.readNonNegativeInt("experimental codec body length"));
    reader.requireEnd();
    long[] values = ExperimentalLongCodec.decode(kind, body);
    switch (representation) {
      case DIRECT_INT32:
        requireType(dataType, TSDataType.INT32, TSDataType.DATE);
        return values;
      case DIRECT_INT64:
        requireType(dataType, TSDataType.INT64, TSDataType.TIMESTAMP);
        return values;
      case RAW_FLOAT:
        requireType(dataType, TSDataType.FLOAT);
        return values;
      case SCALED_FLOAT:
        requireType(dataType, TSDataType.FLOAT);
        for (int i = 0; i < values.length; i++) {
          float value = BigDecimal.valueOf(values[i], scale).floatValue();
          values[i] = Float.floatToRawIntBits(value) & 0xffffffffL;
        }
        return values;
      case RAW_DOUBLE:
        requireType(dataType, TSDataType.DOUBLE);
        return values;
      case SCALED_DOUBLE:
        requireType(dataType, TSDataType.DOUBLE);
        for (int i = 0; i < values.length; i++) {
          double value = BigDecimal.valueOf(values[i], scale).doubleValue();
          values[i] = Double.doubleToRawLongBits(value);
        }
        return values;
      default:
        throw new IllegalArgumentException("unknown experimental numeric representation");
    }
  }

  private static ScaledValues scaleFloatsExactly(long[] rawValues) {
    int scale = 0;
    float[] values = new float[rawValues.length];
    for (int i = 0; i < rawValues.length; i++) {
      values[i] = Float.intBitsToFloat((int) rawValues[i]);
      if (!Float.isFinite(values[i]) || Float.floatToRawIntBits(values[i]) == 0x80000000) {
        return null;
      }
      BigDecimal decimal = new BigDecimal(Float.toString(values[i])).stripTrailingZeros();
      scale = Math.max(scale, Math.max(0, decimal.scale()));
      if (scale > 9) {
        return null;
      }
    }
    long[] scaled = new long[values.length];
    try {
      for (int i = 0; i < values.length; i++) {
        scaled[i] =
            new BigDecimal(Float.toString(values[i])).movePointRight(scale).longValueExact();
        float decoded = BigDecimal.valueOf(scaled[i], scale).floatValue();
        if (Float.floatToRawIntBits(decoded) != Float.floatToRawIntBits(values[i])) {
          return null;
        }
      }
    } catch (ArithmeticException e) {
      return null;
    }
    return new ScaledValues(scale, scaled);
  }

  private static ScaledValues scaleDoublesExactly(long[] rawValues) {
    int scale = 0;
    double[] values = new double[rawValues.length];
    for (int i = 0; i < rawValues.length; i++) {
      values[i] = Double.longBitsToDouble(rawValues[i]);
      if (!Double.isFinite(values[i])
          || Double.doubleToRawLongBits(values[i]) == 0x8000000000000000L) {
        return null;
      }
      BigDecimal decimal = BigDecimal.valueOf(values[i]).stripTrailingZeros();
      scale = Math.max(scale, Math.max(0, decimal.scale()));
      if (scale > 18) {
        return null;
      }
    }
    long[] scaled = new long[values.length];
    try {
      for (int i = 0; i < values.length; i++) {
        scaled[i] = BigDecimal.valueOf(values[i]).movePointRight(scale).longValueExact();
        double decoded = BigDecimal.valueOf(scaled[i], scale).doubleValue();
        if (Double.doubleToRawLongBits(decoded) != Double.doubleToRawLongBits(values[i])) {
          return null;
        }
      }
    } catch (ArithmeticException e) {
      return null;
    }
    return new ScaledValues(scale, scaled);
  }

  private static void requireType(TSDataType actual, TSDataType... expected) {
    for (TSDataType type : expected) {
      if (actual == type) {
        return;
      }
    }
    throw new IllegalArgumentException("experimental payload type mismatch: " + actual);
  }

  private static void putInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xff);
    out.write((value >>> 16) & 0xff);
    out.write((value >>> 8) & 0xff);
    out.write(value & 0xff);
  }

  private static final class ScaledValues {
    private final int scale;
    private final long[] values;

    private ScaledValues(int scale, long[] values) {
      this.scale = scale;
      this.values = values;
    }
  }

  private static final class Reader {
    private final byte[] data;
    private int position;

    private Reader(byte[] data) {
      this.data = data;
    }

    private int readUnsignedByte() {
      require(1);
      return data[position++] & 0xff;
    }

    private int readInt() {
      require(Integer.BYTES);
      int value =
          ((data[position] & 0xff) << 24)
              | ((data[position + 1] & 0xff) << 16)
              | ((data[position + 2] & 0xff) << 8)
              | (data[position + 3] & 0xff);
      position += Integer.BYTES;
      return value;
    }

    private int readNonNegativeInt(String name) {
      int value = readInt();
      if (value < 0) {
        throw new IllegalArgumentException(name + " is negative");
      }
      return value;
    }

    private byte[] readBytes(int length) {
      require(length);
      byte[] bytes = Arrays.copyOfRange(data, position, position + length);
      position += length;
      return bytes;
    }

    private void require(int length) {
      if (length < 0 || position > data.length - length) {
        throw new IllegalArgumentException("truncated experimental numeric payload");
      }
    }

    private void requireEnd() {
      if (position != data.length) {
        throw new IllegalArgumentException("trailing experimental numeric bytes");
      }
    }
  }
}
