/**
/ * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.google.common.base.Preconditions;

/**
 * Writes events the a file give a hadoop uri path. If no uri is specified It
 * defaults to the set by the given configured by fs.default.name config
 * variable
 */
public class DFSEventSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(DFSEventSink.class);
  protected static final String KEY_CLASS_NAME = "keyClassName";
  protected static final String VALUE_CLASS_NAME = "valueClassName";
	protected static final String RENAME = "rename";
  String path;
  Writer writer = null;
  // We keep a - potentially unbounded - set of writers around to deal with
  // different tags on events. Therefore this feature should be used with some
  // care (where the set of possible paths is small) until we do something
  // more sensible with resource management.
  final Map<String, Writer> sfWriters = new HashMap<String, Writer>();

  // Used to short-circuit around doing regex matches when we know there are
  // no templates to be replaced.
  boolean shouldSub = false;
  
  Class<? extends Writable> keyClz;
  Class<? extends Writable> valueClz;

  private boolean rename = false;
  
  public DFSEventSink(String path) {
  	this(path, false);
  }

  public DFSEventSink(String path, boolean rename) {
    this.path = path;
    shouldSub = Event.containsTag(path);
    this.rename = rename;
  }
  
  @SuppressWarnings("unchecked")
  public DFSEventSink(String path, String keyClzName, String valueClzName, boolean rename) {
	  this(path, rename);
	  try {
		  if(keyClzName != null && keyClzName.length() > 0) {
			  keyClz =  (Class<? extends Writable>) Class.forName(keyClzName);
		  } else {
			  keyClz = WriteableEventKey.class;
		  }
	  } catch (ClassNotFoundException e) {
		  LOG.error(keyClzName + " is not found : " + e.getMessage());
		  LOG.info("replace default key class : " + WriteableEventKey.class.getName());
		  keyClz = WriteableEventKey.class;
	  }
	    
	  try {
		  if(valueClzName != null & valueClzName.length() > 0) {
			  valueClz = (Class<? extends Writable>) Class.forName(valueClzName);
		  } else {
			  valueClz = WriteableEvent.class;
		  }	
	  } catch (ClassNotFoundException e) {
		  valueClz = WriteableEvent.class;
	  }
  }

  protected Writer openWriter(String p) throws IOException {
    LOG.info("Opening " + p);
    FlumeConfiguration conf = FlumeConfiguration.get();

    Path dstPath = new Path(p);
    FileSystem hdfs = dstPath.getFileSystem(conf);
        
    Writer w = SequenceFile.createWriter(hdfs, conf, dstPath, keyClz, valueClz);

    return w;
  }

  /**
   * Writes the message to an HDFS file whose path is substituted with tags
   * drawn from the supplied event
   */
  @Override
  public void append(Event e) throws IOException, InterruptedException  {
    Writer w = writer;

    if (shouldSub) {
      String realPath = e.escapeString(path);
      w = sfWriters.get(realPath);
      if (w == null) {
        w = openWriter(realPath);
        sfWriters.put(realPath, w);
      }
    }

    Preconditions.checkState(w != null,
        "Attempted to append to a null dfs writer!");
    
    Writable key = null;
    try {
		key = keyClz.getConstructor(Event.class).newInstance(e);
	} catch (Exception ex) {
		LOG.error(keyClz.getName() + " dose not have contructor with parameter (Event)", ex);
	}
	
	Writable value = null;
	try {
		value = valueClz.getConstructor(Event.class).newInstance(e);
	} catch (Exception ex) {
		LOG.error(valueClz.getName() + " dose not have contructor with parameter (Event)", ex);
	}
	
    w.append(key, value);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
  	Path dstPath = new Path(this.path);
  	FileSystem hdfs = dstPath.getFileSystem(FlumeConfiguration.get());
  	
    if (shouldSub) {
      for (Entry<String, Writer> e : sfWriters.entrySet()) {
        LOG.info("Closing " + e.getKey());
        e.getValue().close();
        
        if(rename) {
        	Path newPath = null; 
        	if(e.getKey().endsWith(".seq")) {
        		newPath = new Path(e.getKey().replace(".seq", ".done"));
        	} else {
        		newPath = new Path(e.getKey() + ".done");
        	}
        	LOG.info("rename [" + new Path(e.getKey()) + "] -> [" + newPath + "]");
        	hdfs.rename(new Path(e.getKey()), newPath);
        }
        
      }
    } else {

      if (writer == null) {
        LOG.warn("DFS Sink double closed? " + path);
        return;
      }
      LOG.info("Closing " + path);
      writer.close();
      
      if(rename) {
      	Path newPath = null; 
      	if(this.path.endsWith(".seq")) {
      		newPath = new Path(this.path.replace(".seq", ".done"));
      	} else {
      		newPath = new Path(this.path + ".done");
      	}
      	LOG.info("rename [" + dstPath + "] -> [" + newPath + "]");
        hdfs.rename(dstPath, newPath);
      }

      writer = null;
    }
  }

  @Override
  public void open() throws IOException {
    if (!shouldSub) {
      writer = openWriter(path);
    }
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length != 1) {
          // TODO (jon) make this message easier.
          throw new IllegalArgumentException(
              "usage: dfs(\"[(hdfs|file|s3n|...)://namenode[:port]]/path\" " +
              "{, keyClassName=\"KeyClassName\", valueClassName=\"ValueClassName\"}) ");
        }
        
        String keyClassName = context.getValue(KEY_CLASS_NAME);
        String valueClassName = context.getValue(VALUE_CLASS_NAME);

        boolean rename = context.getValue(RENAME) != null ? Boolean.parseBoolean(context.getValue(RENAME)) : false;
        
        if(keyClassName != null && keyClassName.length() > 0 
        		&& valueClassName != null && valueClassName.length() >0) {
        	return new DFSEventSink(args[0], keyClassName, valueClassName, rename);
        } else {
        	return new DFSEventSink(args[0], rename);
        }
      }
    };
  }
}
