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
package org.apache.tsfile.write;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileOutput;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class CsvReadWriteTest {

  private static final String PARENT_DIR = "D://github/xjz17/subcolumn/";
  private static final String INPUT_PARENT_DIR = PARENT_DIR + "dataset/";
  private static final String OUTPUT_PARENT_DIR = PARENT_DIR + "result/tsfile_read_write/";
  private static final String TSFILE_OUTPUT_DIR = OUTPUT_PARENT_DIR + "tsfiles/";
  private static final String RESULT_CSV_PATH = OUTPUT_PARENT_DIR + "write_time.csv";
  private static final String READ_RESULT_CSV_PATH = OUTPUT_PARENT_DIR + "read_time.csv";
  private static final int REPEAT_TIMES = 100;
  private static final String DEVICE_NAME = "device_1";
  private static final String MEASUREMENT_NAME = "sensor_1";
  private static final int MAX_DECIMAL_PRECISION = 8;
  private static final int FILE_OUTPUT_BUFFER_SIZE = 8192;
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean THREAD_CPU_TIME_AVAILABLE = initializeThreadCpuTime();

  private static final List<TSEncoding> ENCODINGS =
      Arrays.asList(
          TSEncoding.SUBCOLUMN,
          TSEncoding.TS_2DIFF,
          TSEncoding.RLE,
          TSEncoding.GORILLA,
          TSEncoding.CHIMP);

  private final IDeviceID deviceID = Factory.DEFAULT_FACTORY.create(DEVICE_NAME);

  @Test
  public void benchmarkCsvWriteEncodings() throws Exception {
    File inputDir = new File(INPUT_PARENT_DIR);
    File outputDir = new File(OUTPUT_PARENT_DIR);
    File tsFileDir = new File(TSFILE_OUTPUT_DIR);
    if (!inputDir.exists()) {
      throw new IOException("Input dataset directory does not exist: " + INPUT_PARENT_DIR);
    }
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    if (!tsFileDir.exists()) {
      tsFileDir.mkdirs();
    }

    File[] csvFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
    if (csvFiles == null || csvFiles.length == 0) {
      throw new IOException("No CSV dataset files found under " + INPUT_PARENT_DIR);
    }
    Arrays.sort(csvFiles, Comparator.comparing(File::getName));

    CsvWriter writer = new CsvWriter(RESULT_CSV_PATH, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Write Total Time Nanos",
          "Write CPU Time Nanos",
          "Write IO Time Nanos",
          "Write IO Write Nanos",
          "Write IO Flush Nanos",
          "Write IO Force Nanos",
          "Points",
          "Max Decimal Precision",
          "Multiplier",
          "TsFile Size Bytes",
          "TsFile Path"
        });

    try {
      for (File datasetFile : csvFiles) {
        ArrayList<Double> rawValues = new ArrayList<>();
        int maxDecimalPrecision = 0;

        InputStream inputStream = Files.newInputStream(datasetFile.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        try {
          while (loader.readRecord()) {
            String[] values = loader.getValues();
            if (values.length == 0) {
              continue;
            }

            String value = values[0].trim();
            if (value.isEmpty()) {
              continue;
            }

            maxDecimalPrecision = Math.max(maxDecimalPrecision, getDecimalPrecision(value));
            rawValues.add(Double.valueOf(value));
          }
        } finally {
          loader.close();
          inputStream.close();
        }

        int boundedPrecision = Math.min(maxDecimalPrecision, MAX_DECIMAL_PRECISION);
        long multiplier = (long) Math.pow(10, boundedPrecision);
        int[] processedValues = new int[rawValues.size()];
        for (int i = 0; i < rawValues.size(); i++) {
          processedValues[i] = (int) (rawValues.get(i) * multiplier);
        }

        String datasetName = extractFileName(datasetFile.getName());
        for (TSEncoding encoding : ENCODINGS) {
          java.nio.file.Path tsFilePath =
              Paths.get(TSFILE_OUTPUT_DIR, datasetName + "_" + encoding.name().toLowerCase() + ".tsfile");
          WriteBenchmarkResult benchmarkResult =
              benchmarkWrite(tsFilePath.toFile(), encoding, processedValues);

          writer.writeRecord(
              new String[] {
                datasetName,
                encoding.name(),
                String.valueOf(benchmarkResult.getTotalTimeNanos()),
                String.valueOf(benchmarkResult.getCpuTimeNanos()),
                String.valueOf(benchmarkResult.getIoTimeNanos()),
                String.valueOf(benchmarkResult.getIoWriteNanos()),
                String.valueOf(benchmarkResult.getIoFlushNanos()),
                String.valueOf(benchmarkResult.getIoForceNanos()),
                String.valueOf(processedValues.length),
                String.valueOf(boundedPrecision),
                String.valueOf(multiplier),
                String.valueOf(benchmarkResult.getTsFileSizeBytes()),
                tsFilePath.toString()
              });
        }
      }
    } finally {
      writer.close();
    }
  }

  @Test
  public void benchmarkTsFileReadEncodings() throws Exception {
    File tsFileDir = new File(TSFILE_OUTPUT_DIR);
    if (!tsFileDir.exists()) {
      throw new IOException("TsFile directory does not exist: " + TSFILE_OUTPUT_DIR);
    }

    File[] tsFiles = tsFileDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".tsfile"));
    if (tsFiles == null || tsFiles.length == 0) {
      throw new IOException("No tsfile files found under " + TSFILE_OUTPUT_DIR);
    }
    Arrays.sort(tsFiles, Comparator.comparing(File::getName));

    CsvWriter writer = new CsvWriter(READ_RESULT_CSV_PATH, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Read Time Nanos",
          "Points",
          "TsFile Size Bytes",
          "TsFile Path"
        });

    try {
      for (File tsFile : tsFiles) {
        long elapsedNanos = benchmarkRead(tsFile);
        long pointCount = readTsFile(tsFile);
        String fileName = extractFileName(tsFile.getName());
        int splitIndex = fileName.lastIndexOf('_');
        String datasetName = splitIndex >= 0 ? fileName.substring(0, splitIndex) : fileName;
        String encodingName = splitIndex >= 0 ? fileName.substring(splitIndex + 1) : "unknown";

        writer.writeRecord(
            new String[] {
              datasetName,
              encodingName.toUpperCase(),
              String.valueOf(elapsedNanos),
              String.valueOf(pointCount),
              String.valueOf(Files.size(tsFile.toPath())),
              tsFile.toPath().toString()
            });
      }
    } finally {
      writer.close();
    }
  }

  private WriteBenchmarkResult benchmarkWrite(File tsFile, TSEncoding encoding, int[] processedValues)
      throws IOException, WriteProcessException {
    long totalTimeNanos = 0;
    long totalCpuTimeNanos = 0;
    long totalIoWriteNanos = 0;
    long totalIoFlushNanos = 0;
    long totalIoForceNanos = 0;

    for (int repeat = 0; repeat < REPEAT_TIMES; repeat++) {
      Files.deleteIfExists(tsFile.toPath());
      WriteBenchmarkResult singleRunResult = writeTsFile(tsFile, encoding, processedValues);
      totalTimeNanos += singleRunResult.getTotalTimeNanos();
      totalCpuTimeNanos += singleRunResult.getCpuTimeNanos();
      totalIoWriteNanos += singleRunResult.getIoWriteNanos();
      totalIoFlushNanos += singleRunResult.getIoFlushNanos();
      totalIoForceNanos += singleRunResult.getIoForceNanos();
    }

    long tsFileSizeBytes = Files.size(tsFile.toPath());
    return new WriteBenchmarkResult(
        totalTimeNanos / REPEAT_TIMES,
        totalCpuTimeNanos / REPEAT_TIMES,
        totalIoWriteNanos / REPEAT_TIMES,
        totalIoFlushNanos / REPEAT_TIMES,
        totalIoForceNanos / REPEAT_TIMES,
        tsFileSizeBytes);
  }

  private long benchmarkRead(File tsFile) throws IOException {
    long startNanos = System.nanoTime();
    for (int repeat = 0; repeat < REPEAT_TIMES; repeat++) {
      readTsFile(tsFile);
    }
    return (System.nanoTime() - startNanos) / REPEAT_TIMES;
  }

  private WriteBenchmarkResult writeTsFile(File tsFile, TSEncoding encoding, int[] processedValues)
      throws IOException, WriteProcessException {
    ProfilingTsFileOutput profilingOutput = new ProfilingTsFileOutput(tsFile);
    long startNanos = System.nanoTime();
    long startCpuTimeNanos = currentThreadCpuTime();

    try (TsFileWriter tsFileWriter = new TsFileWriter(profilingOutput, new Schema())) {
      tsFileWriter.registerTimeseries(
          new Path(deviceID),
          new MeasurementSchema(MEASUREMENT_NAME, TSDataType.INT32, encoding));
      for (int i = 0; i < processedValues.length; i++) {
        TSRecord record = new TSRecord(deviceID, i + 1L);
        record.addTuple(new IntDataPoint(MEASUREMENT_NAME, processedValues[i]));
        tsFileWriter.writeRecord(record);
      }
    }

    long totalTimeNanos = System.nanoTime() - startNanos;
    long cpuTimeNanos = currentThreadCpuTime() - startCpuTimeNanos;
    return new WriteBenchmarkResult(
        totalTimeNanos,
        cpuTimeNanos,
        profilingOutput.getIoWriteNanos(),
        profilingOutput.getIoFlushNanos(),
        profilingOutput.getIoForceNanos(),
        Files.size(tsFile.toPath()));
  }

  private long readTsFile(File tsFile) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getPath());
        TsFileReader tsFileReader = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(deviceID, MEASUREMENT_NAME, true));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = tsFileReader.query(queryExpression);

      long pointCount = 0;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        if (!rowRecord.getFields().isEmpty() && rowRecord.getFields().get(0) != null) {
          pointCount++;
        }
      }
      return pointCount;
    }
  }

  private static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf('.');
    if (decimalIndex == -1) {
      return 0;
    }
    return str.substring(decimalIndex + 1).length();
  }

  private static String extractFileName(String path) {
    File file = new File(path);
    String fileName = file.getName();
    int dotIndex = fileName.lastIndexOf('.');
    if (dotIndex <= 0) {
      return fileName;
    }
    return fileName.substring(0, dotIndex);
  }

  private static boolean initializeThreadCpuTime() {
    if (!THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported()) {
      return false;
    }
    if (!THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
      try {
        THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
      } catch (UnsupportedOperationException | SecurityException e) {
        return false;
      }
    }
    return THREAD_MX_BEAN.isThreadCpuTimeEnabled();
  }

  private static long currentThreadCpuTime() {
    return THREAD_CPU_TIME_AVAILABLE ? THREAD_MX_BEAN.getCurrentThreadCpuTime() : -1L;
  }

  private static final class WriteBenchmarkResult {
    private final long totalTimeNanos;
    private final long cpuTimeNanos;
    private final long ioWriteNanos;
    private final long ioFlushNanos;
    private final long ioForceNanos;
    private final long tsFileSizeBytes;

    private WriteBenchmarkResult(
        long totalTimeNanos,
        long cpuTimeNanos,
        long ioWriteNanos,
        long ioFlushNanos,
        long ioForceNanos,
        long tsFileSizeBytes) {
      this.totalTimeNanos = totalTimeNanos;
      this.cpuTimeNanos = cpuTimeNanos;
      this.ioWriteNanos = ioWriteNanos;
      this.ioFlushNanos = ioFlushNanos;
      this.ioForceNanos = ioForceNanos;
      this.tsFileSizeBytes = tsFileSizeBytes;
    }

    private long getTotalTimeNanos() {
      return totalTimeNanos;
    }

    private long getCpuTimeNanos() {
      return cpuTimeNanos;
    }

    private long getIoTimeNanos() {
      return ioWriteNanos + ioFlushNanos + ioForceNanos;
    }

    private long getIoWriteNanos() {
      return ioWriteNanos;
    }

    private long getIoFlushNanos() {
      return ioFlushNanos;
    }

    private long getIoForceNanos() {
      return ioForceNanos;
    }

    private long getTsFileSizeBytes() {
      return tsFileSizeBytes;
    }
  }

  private static final class ProfilingTsFileOutput extends OutputStream implements TsFileOutput {
    private final FileOutputStream outputStream;
    private final byte[] buffer = new byte[FILE_OUTPUT_BUFFER_SIZE];

    private int bufferedByteCount;
    private long position;
    private long ioWriteNanos;
    private long ioFlushNanos;
    private long ioForceNanos;
    private boolean closed;

    private ProfilingTsFileOutput(File file) throws IOException {
      this.outputStream = new FileOutputStream(file);
    }

    @Override
    public void write(int b) throws IOException {
      ensureOpen();
      if (bufferedByteCount >= buffer.length) {
        flushBuffer();
      }
      buffer[bufferedByteCount++] = (byte) b;
      position++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte b) throws IOException {
      write(b & 0xFF);
    }

    @Override
    public void write(byte[] b, int start, int offset) throws IOException {
      ensureOpen();
      if (offset >= buffer.length) {
        flushBuffer();
        long ioStartNanos = System.nanoTime();
        outputStream.write(b, start, offset);
        ioWriteNanos += System.nanoTime() - ioStartNanos;
        position += offset;
        return;
      }

      if (offset > buffer.length - bufferedByteCount) {
        flushBuffer();
      }
      System.arraycopy(b, start, buffer, bufferedByteCount, offset);
      bufferedByteCount += offset;
      position += offset;
    }

    @Override
    public void write(ByteBuffer byteBuffer) throws IOException {
      if (byteBuffer.hasArray()) {
        write(
            byteBuffer.array(),
            byteBuffer.arrayOffset() + byteBuffer.position(),
            byteBuffer.remaining());
        byteBuffer.position(byteBuffer.limit());
        return;
      }

      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      write(bytes, 0, bytes.length);
    }

    @Override
    public long getPosition() {
      return position;
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      flushBuffer();
      outputStream.close();
      closed = true;
    }

    @Override
    public OutputStream wrapAsStream() {
      return this;
    }

    @Override
    public void flush() throws IOException {
      ensureOpen();
      flushBuffer();
      long ioStartNanos = System.nanoTime();
      outputStream.flush();
      ioFlushNanos += System.nanoTime() - ioStartNanos;
    }

    @Override
    public void truncate(long size) throws IOException {
      ensureOpen();
      flushBuffer();
      outputStream.getChannel().truncate(size);
      position = outputStream.getChannel().position();
    }

    @Override
    public void force() throws IOException {
      ensureOpen();
      flush();
      long ioStartNanos = System.nanoTime();
      outputStream.getFD().sync();
      ioForceNanos += System.nanoTime() - ioStartNanos;
    }

    private void flushBuffer() throws IOException {
      if (bufferedByteCount == 0) {
        return;
      }
      long ioStartNanos = System.nanoTime();
      outputStream.write(buffer, 0, bufferedByteCount);
      ioWriteNanos += System.nanoTime() - ioStartNanos;
      bufferedByteCount = 0;
    }

    private void ensureOpen() throws IOException {
      if (closed) {
        throw new IOException("TsFile output has been closed.");
      }
    }

    private long getIoWriteNanos() {
      return ioWriteNanos;
    }

    private long getIoFlushNanos() {
      return ioFlushNanos;
    }

    private long getIoForceNanos() {
      return ioForceNanos;
    }
  }

}