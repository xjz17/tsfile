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
package org.apache.tsfile;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** End-to-end numeric write/read benchmark over physical TsFile files. */
public final class SystemCodecBenchmark {

  private static final byte[] INPUT_MAGIC = "SCDBIN01".getBytes(StandardCharsets.US_ASCII);
  private static final String DEVICE = "root.system_codec_bench";
  private static final List<TSEncoding> ENCODINGS =
      Arrays.asList(
          TSEncoding.BOS,
          TSEncoding.SUBCOLUMN,
          TSEncoding.PLAIN,
          TSEncoding.GORILLA_V1,
          TSEncoding.GORILLA,
          TSEncoding.CHIMP,
          TSEncoding.RLBE,
          TSEncoding.CAMEL);

  private SystemCodecBenchmark() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 5) {
      throw new IllegalArgumentException(
          "usage: SystemCodecBenchmark <input.scdbin> <output-dir> <dataset> [iterations] [warmups]");
    }
    Dataset dataset = readDataset(java.nio.file.Path.of(args[0]));
    java.nio.file.Path outputDirectory = java.nio.file.Path.of(args[1]);
    Files.createDirectories(outputDirectory);
    int iterations = args.length >= 4 ? Integer.parseInt(args[3]) : 3;
    int warmups = args.length >= 5 ? Integer.parseInt(args[4]) : 1;
    if (iterations <= 0 || warmups <= 0) {
      throw new IllegalArgumentException("iterations and warmups must be positive");
    }
    System.out.println(
        "system,dataset,codec,iteration,rows,columns,write_ms,read_ms,file_bytes,hash_ok");
    for (int iteration = -warmups; iteration < iterations; iteration++) {
      int start = Math.floorMod(iteration + warmups, ENCODINGS.size());
      for (int step = 0; step < ENCODINGS.size(); step++) {
        TSEncoding encoding = ENCODINGS.get((start + step) % ENCODINGS.size());
        java.nio.file.Path path =
            outputDirectory.resolve(
                sanitize(args[2])
                    + "_"
                    + encoding.name().toLowerCase()
                    + "_"
                    + iteration
                    + ".tsfile");
        Result result = runOnce(path.toFile(), dataset, encoding);
        Files.deleteIfExists(path);
        if (iteration >= 0) {
          System.out.printf(
              "tsfile,%s,%s,%d,%d,%d,%.6f,%.6f,%d,%s%n",
              args[2],
              encoding,
              iteration,
              dataset.rows,
              dataset.columns.length,
              result.writeNanos / 1_000_000.0,
              result.readNanos / 1_000_000.0,
              result.fileBytes,
              result.hashOk);
        }
      }
    }
  }

  private static Result runOnce(File file, Dataset dataset, TSEncoding encoding) throws Exception {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    List<Path> selectedPaths = new ArrayList<>();
    for (int column = 0; column < dataset.columns.length; column++) {
      String name = "c" + column;
      schemas.add(
          new MeasurementSchema(name, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED));
      selectedPaths.add(new Path(DEVICE, name, true));
    }

    long writeStart = System.nanoTime();
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(DEVICE), schemas);
      Tablet tablet = new Tablet(DEVICE, schemas);
      for (int sourceRow = 0; sourceRow < dataset.rows; sourceRow++) {
        int tabletRow = tablet.getRowSize();
        tablet.addTimestamp(tabletRow, sourceRow);
        for (int column = 0; column < dataset.columns.length; column++) {
          tablet.addValue(tabletRow, column, dataset.columns[column][sourceRow]);
        }
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTree(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() != 0) {
        writer.writeTree(tablet);
      }
    }
    try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
      channel.force(true);
    }
    long writeNanos = System.nanoTime() - writeStart;
    long fileBytes = file.length();

    boolean hashOk = true;
    int row = 0;
    long readStart = System.nanoTime();
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(sequenceReader)) {
      QueryDataSet query = reader.query(QueryExpression.create(selectedPaths, null));
      while (query.hasNext()) {
        RowRecord record = query.next();
        if (record.getTimestamp() != row) {
          hashOk = false;
        }
        List<Field> fields = record.getFields();
        for (int column = 0; column < dataset.columns.length; column++) {
          long expected = Double.doubleToRawLongBits(dataset.columns[column][row]);
          long actual = Double.doubleToRawLongBits(fields.get(column).getDoubleV());
          hashOk &= expected == actual;
        }
        row++;
      }
    }
    long readNanos = System.nanoTime() - readStart;
    hashOk &= row == dataset.rows;
    return new Result(writeNanos, readNanos, fileBytes, hashOk);
  }

  private static Dataset readDataset(java.nio.file.Path path) throws IOException {
    try (DataInputStream input =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
      byte[] magic = input.readNBytes(INPUT_MAGIC.length);
      if (!Arrays.equals(magic, INPUT_MAGIC)) {
        throw new IOException("invalid SCDBIN input magic");
      }
      int columnCount = input.readInt();
      long rowCount = input.readLong();
      if (columnCount <= 0 || rowCount < 0 || rowCount > Integer.MAX_VALUE) {
        throw new IOException("invalid SCDBIN dimensions");
      }
      for (int column = 0; column < columnCount; column++) {
        int nameLength = input.readUnsignedShort();
        if (input.readNBytes(nameLength).length != nameLength) {
          throw new EOFException("truncated SCDBIN column name");
        }
      }
      double[][] columns = new double[columnCount][(int) rowCount];
      for (int column = 0; column < columnCount; column++) {
        for (int row = 0; row < rowCount; row++) {
          columns[column][row] = Double.longBitsToDouble(input.readLong());
        }
      }
      if (input.read() != -1) {
        throw new IOException("trailing SCDBIN bytes");
      }
      return new Dataset((int) rowCount, columns);
    }
  }

  private static String sanitize(String value) {
    return value.replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  private static final class Dataset {
    private final int rows;
    private final double[][] columns;

    private Dataset(int rows, double[][] columns) {
      this.rows = rows;
      this.columns = columns;
    }
  }

  private static final class Result {
    private final long writeNanos;
    private final long readNanos;
    private final long fileBytes;
    private final boolean hashOk;

    private Result(long writeNanos, long readNanos, long fileBytes, boolean hashOk) {
      this.writeNanos = writeNanos;
      this.readNanos = readNanos;
      this.fileBytes = fileBytes;
      this.hashOk = hashOk;
    }
  }
}
