/**
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
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

package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.cassandra.hadoop.cql3.CqlRecordReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class CqlHiveRecordReader extends RecordReader<MapWritableComparable, MapWritable>
        implements org.apache.hadoop.mapred.RecordReader<MapWritableComparable, MapWritable> {

  static final Logger LOG = LoggerFactory.getLogger(CqlHiveRecordReader.class);

  //private final boolean isTransposed;
  private final CqlRecordReader cfrr;
  private Iterator<Map.Entry<String, ByteBuffer>> columnIterator = null;
  private Map.Entry<String, ByteBuffer> currentEntry;
  //private Iterator<IColumn> subColumnIterator = null;
  private MapWritableComparable currentKey = null;
  private final MapWritable currentValue = new MapWritable();

  public CqlHiveRecordReader(CqlRecordReader cprr) { //, boolean isTransposed) {
    this.cfrr = cprr;
    //this.isTransposed = isTransposed;
  }

  @Override
  public void close() throws IOException {
    cfrr.close();
  }

  @Override
  public MapWritableComparable createKey() {
    return new MapWritableComparable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return cfrr.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return cfrr.getProgress();
  }

  public static int callCount = 0;

  @Override
  public boolean next(MapWritableComparable key, MapWritable value) throws IOException {
    if (!nextKeyValue()) {
      return false;
    }

    key.clear();
    key.putAll(getCurrentKey());

    value.clear();
    value.putAll(getCurrentValue());

    return true;
  }

  @Override
  public MapWritableComparable getCurrentKey() {
    return currentKey;
  }

  @Override
  public MapWritable getCurrentValue() {
    return currentValue;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    cfrr.initialize(split, context);
  }

  private BytesWritable convertByteBuffer(ByteBuffer val) {
    return new BytesWritable(ByteBufferUtil.getArray(val));
  }

  @Override
  public boolean nextKeyValue() throws IOException {

    boolean next = false;

    // In the case that we are transposing we create a fixed set of columns
    // per cassandra column
    next = cfrr.nextKeyValue();

    currentValue.clear();

    if (next) {
      currentKey = mapToMapWritable(cfrr.getCurrentKey());

      // rowKey
      currentValue.putAll(currentKey);
      currentValue.putAll(mapToMapWritable(cfrr.getCurrentValue()));
      //populateMap(cfrr.getCurrentValue(), currentValue);
    }

    return next;
  }

  private MapWritableComparable mapToMapWritable(Map<String, ByteBuffer> map) {
    MapWritableComparable mw = new MapWritableComparable();
    for (Map.Entry<String, ByteBuffer> e : map.entrySet()) {
      mw.put(new Text(e.getKey()), convertByteBuffer(e.getValue()));
    }
    return mw;
  }

/*  private void populateMap(Map<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cvalue, MapWritable value) {
    for (Map.Entry<Map<String, ByteBuffer>, Map<String, ByteBuffer>> e : cvalue.entrySet()) {
      Map<String, ByteBuffer> k = e.getKey();
      Map<String, ByteBuffer> v = e.getValue();


      if (!v.isLive()) {
        continue;
      }


      BytesWritable newKey = k);
      BytesWritable newValue = convertByteBuffer(v.value());

      value.put(newKey, newValue);
    }
  } */
}
