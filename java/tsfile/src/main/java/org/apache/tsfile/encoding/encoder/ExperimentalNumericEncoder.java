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

import org.apache.tsfile.encoding.common.ExperimentalLongCodec;
import org.apache.tsfile.encoding.common.ExperimentalNumericPageCodec;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/** Shared page buffer for the experimental BOS and Sub-column numeric encoders. */
abstract class ExperimentalNumericEncoder extends Encoder {

  private final ExperimentalLongCodec.Kind kind;
  private final TSDataType dataType;
  private long[] values = new long[128];
  private int size;

  ExperimentalNumericEncoder(
      TSEncoding encoding, ExperimentalLongCodec.Kind kind, TSDataType dataType) {
    super(encoding);
    this.kind = kind;
    this.dataType = dataType;
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    append(value);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    append(value);
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    append(Float.floatToRawIntBits(value) & 0xffffffffL);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    append(Double.doubleToRawLongBits(value));
  }

  private void append(long value) {
    if (size == values.length) {
      values = Arrays.copyOf(values, values.length * 2);
    }
    values[size++] = value;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    byte[] payload = ExperimentalNumericPageCodec.encode(kind, dataType, values, size);
    out.write(payload);
    size = 0;
  }

  @Override
  public int getOneItemMaxSize() {
    return Long.BYTES * 2 + 1;
  }

  @Override
  public long getMaxByteSize() {
    return (long) size * Long.BYTES;
  }
}
