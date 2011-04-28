/**
 * Licensed to Cloudera, Inc. under one
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
package com.cloudera.flume.handlers.text;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.CustomDelimCursor.DelimMode;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Pair;
import com.cloudera.util.dirwatcher.DirChangeHandler;
import com.cloudera.util.dirwatcher.DirWatcher;
import com.cloudera.util.dirwatcher.RegexFileFilter;
import com.google.common.base.Preconditions;
import com.nexr.agent.cp.CheckPointManager;

/**
 * This source tails all the file in a directory that match a specified regular
 * expression.
 */
public class TailDirSource extends EventSource.Base {
  public static final Logger LOG = LoggerFactory.getLogger(TailDirSource.class);
  public static final String USAGE = "usage: tailDir(\"dirname\"[, fileregex=\".*\"[, startFromEnd=false[, recurseDepth=0]]])";
  private DirWatcher watcher;
  private ConcurrentMap<String, DirWatcher> subdirWatcherMap;
  private TailSource tail;
  final private File dir;
  final private String regex;
  private final boolean startFromEnd;
  private final int recurseDepth;

  final private String delimRegex;
  final private DelimMode delimMode;
  
  private CheckPointManager checkPointManager;
  private Map<String, Long> checkPointOffsetMap;
  

  // Indicates whether dir was checked. It is false before source is open
  // and set to true after the first check of a dir
  private volatile boolean dirChecked = false;

  final private AtomicLong filesAdded = new AtomicLong();
  final private AtomicLong filesDeleted = new AtomicLong();
  final private AtomicLong subdirsAdded = new AtomicLong();
  final private AtomicLong subdirsDeleted = new AtomicLong();

  final public static String A_FILESADDED = "filesAdded";
  final public static String A_FILESDELETED = "filesDeleted";
  final public static String A_FILESPRESENT = "filesPresent";
  final public static String A_SUBDIRSADDED = "subdirsAdded";
  final public static String A_SUBDIRSDELETED = "subdirsDeleted";

  private boolean useCheckpoint = false;
  
  public TailDirSource(File f, String regex) {
    this(f, regex, false);
  }

  public TailDirSource(File f, String regex, boolean startFromEnd) {
    this(f, regex, startFromEnd, 0);
  }

  public TailDirSource(File f, String regex, boolean startFromEnd,
      int recurseDepth) {
    Preconditions.checkArgument(f != null, "File should not be null!");
    Preconditions.checkArgument(regex != null,
        "Regex filter should not be null");

    this.dir = f;
    this.regex = regex;
    this.startFromEnd = startFromEnd;
    this.recurseDepth = recurseDepth;

    // 100 ms between checks
    this.tail = new TailSource(100);
    this.delimMode = null;
    this.delimRegex = null;
  }

  public TailDirSource(File f, String regex, boolean startFromEnd,
      int recurseDepth, String delimRegex, DelimMode delimMode) {
    Preconditions.checkArgument(f != null, "File should not be null!");
    Preconditions.checkArgument(regex != null,
        "Regex filter should not be null");

    this.dir = f;
    this.regex = regex;
    this.startFromEnd = startFromEnd;
    this.recurseDepth = recurseDepth;

    // 100 ms between checks
    this.tail = new TailSource(100);
    this.delimRegex = delimRegex;
    this.delimMode = delimMode;
  }
  
  /**
   * @param logicalNodeName
   */
  protected void initCheckPoint(String logicalNodeName) {
	  if(checkPointManager == null) {
		  this.checkPointOffsetMap =  FlumeNode.getInstance().getCheckPointManager().getOffset(logicalNodeName);
	  } else {
		  this.checkPointOffsetMap = checkPointManager.getOffset(logicalNodeName);
	  }
	  this.useCheckpoint = true;
  }
  
  /**
   * Must be synchronized to isolate watcher
   */
  @Override
  synchronized public void open() throws IOException {
    Preconditions.checkState(watcher == null,
        "Attempting to open an already open TailDirSource (" + dir + ", \""
            + regex + "\")");
    subdirWatcherMap = new ConcurrentHashMap<String, DirWatcher>();
    watcher = createWatcher(dir, regex, recurseDepth);
    dirChecked = true;
    watcher.start();
    tail.open();
  }

  private DirWatcher createWatcher(File dir, final String regex,
      final int recurseDepth) {
    // 250 ms between checks
    DirWatcher watcher = new DirWatcher(dir, new RegexFileFilter(regex), 250);
    watcher.addHandler(new DirChangeHandler() {
      Map<String, Cursor> curmap = new HashMap<String, Cursor>();

      @Override
      public void fileCreated(File f) {
        if (f.isDirectory()) {
          if (recurseDepth <= 0) {
            LOG.debug("Tail dir will not read or recurse "
                + "into subdirectory " + f + ", this watcher recurseDepth: "
                + recurseDepth);
            return;
          }

          LOG.info("added dir " + f + ", recurseDepth: " + (recurseDepth - 1));
          DirWatcher watcher = createWatcher(f, regex, recurseDepth - 1);
          watcher.start();
          subdirWatcherMap.put(f.getPath(), watcher);
          subdirsAdded.incrementAndGet();
          return;
        }

        // Add a new file to the multi tail.
        LOG.info("added file " + f);
        Cursor c;
        if (delimRegex == null) {
          if (startFromEnd && !dirChecked) {
            // init cursor positions on first dir check when startFromEnd is set
            // to true
            c = new Cursor(tail.sync, f, f.length(), f.length(), f
                .lastModified());
            try {
              c.initCursorPos();
            } catch (InterruptedException e) {
              LOG.error("Initializing of cursor failed", e);
              c.close();
              return;
            }
          } else {
        	  if(checkPointOffsetMap != null && checkPointOffsetMap.containsKey(f.getName())) {
        		  long checkPointOffset = checkPointOffsetMap.get(f.getName());
        		  if(checkPointOffset > f.length()) {
        			  LOG.warn("Invalid checkpoint offset : checkpoint offset is larger than " +
        			  		"file length[checkpoint : " + checkPointOffset + ", fileLength : " + f.length()
        			  		+", fileName : " + f.getName() +"]");
        			  checkPointOffset = f.length();
        		  }
        		  c = new Cursor(tail.sync, f, checkPointOffsetMap.get(f.getName()), 
        				  f.length(), f.lastModified(), true);
        		  try {
  					c.initCursorPos();
          		  } catch (InterruptedException e) {
          			  LOG.error("Initializing of custom delimiter cursor failed", e);
          			  c.close();
          			  //TODO 체크포인트 offset으로 가는 것이 실패 했을 경우 어떻게 처리 해야 하는가?
          			  return;
          		  }
        		  
        		  checkPointOffsetMap.remove(f.getName());
        	  } else {
        		  c = new Cursor(tail.sync, f, 0, 0, 0, useCheckpoint);
        	  }
          }
        } else {
          // special delimiter modes
          if (startFromEnd && !dirChecked) {
            // init cursor positions on first dir check when startFromEnd is set
            // to true
            c = new CustomDelimCursor(tail.sync, f, f.length(), f.length(), f
                .lastModified(), delimRegex, delimMode);
            try {
              c.initCursorPos();
            } catch (InterruptedException e) {
              LOG.error("Initializing of custom delimiter cursor failed", e);
              c.close();
              return;
            }
          } else {
        	  if(checkPointOffsetMap != null && checkPointOffsetMap.containsKey(f.getName())) {
        		  long checkPointOffset = checkPointOffsetMap.get(f.getName());
        		  if(checkPointOffset > f.length()) {
        			  LOG.warn("Invalid checkpoint offset : checkpoint offset is larger than " +
        			  		"file length[checkpoint : " + checkPointOffset + ", fileLength : " + f.length()
        			  		+", fileName : " + f.getName() +"]");
        			  checkPointOffset = f.length();
        		  }
        		  c = new CustomDelimCursor(tail.sync, f, checkPointOffsetMap.get(f.getName()), 
        				  f.length(), f.lastModified(), delimRegex, delimMode, true);
        		  try {
					c.initCursorPos();
        		  } catch (InterruptedException e) {
        			  LOG.error("Initializing of custom delimiter cursor failed", e);
        			  c.close();
        			  //TODO 체크포인트 offset으로 가는 것이 실패 했을 경우 어떻게 처리 해야 하는가?
        			  return;
        		  }
        		  
        		  checkPointOffsetMap.remove(f.getName());
        	  } else {
        		  c = new CustomDelimCursor(tail.sync, f, delimRegex, delimMode);
        	  }
          }
        }

        curmap.put(f.getPath(), c);
        tail.addCursor(c);
        filesAdded.incrementAndGet();
      }

      @Override
      public void fileDeleted(File f) {
        LOG.debug("handling deletion of file " + f);
        String fileName = f.getPath();
        // we cannot just check here with f.isDirectory() because f was deleted
        // and f.isDirectory() will return false always
        DirWatcher watcher = subdirWatcherMap.remove(fileName);
        if (watcher != null) {
          LOG.info("removed dir " + f);
          LOG.info("stopping watcher for dir: " + f);
          // stop is not thread-safe, but since this watcher belongs only to
          // this current thread it is safe to call it
          watcher.stop();
          // calling check explicitly to notify about deleted subdirs,
          // so that subdirs watchers can be stopped
          watcher.check();
          subdirsDeleted.incrementAndGet();
          return;
        }

        Cursor c = curmap.remove(fileName);
        // this check may seem unneeded but there are cases which it handles,
        // e.g. if unwatched subdirectory was removed c is null.
        if (c != null) {
          LOG.info("removed file " + f);
          tail.removeCursor(c);
          filesDeleted.incrementAndGet();
        }
      }

    });

    // Separate check is needed to init cursor positions
    // (to the end of the files in dir)
    if (startFromEnd) {
      watcher.check();
    }
    return watcher;
  }

  /**
   * Must be synchronized to isolate watcher
   */
  @Override
  synchronized public void close() throws IOException {
    tail.close();
    this.watcher.stop();
    this.watcher = null;
    for (DirWatcher watcher : subdirWatcherMap.values()) {
      watcher.stop();
    }
    subdirWatcherMap = null;
  }

  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setLongMetric(A_FILESADDED, filesAdded.get());
    rpt.setLongMetric(A_FILESDELETED, filesDeleted.get());
    rpt.setLongMetric(A_SUBDIRSADDED, subdirsAdded.get());
    rpt.setLongMetric(A_SUBDIRSDELETED, subdirsDeleted.get());
    rpt.setLongMetric(A_FILESPRESENT, tail.cursors.size());
    return rpt;
  }

  @Override
  public Event next() throws IOException {
    // this cannot be in synchronized because it has a
    // blocking call to a queue inside it.
    Event e = tail.next();

    synchronized (this) {
      updateEventProcessingStats(e);
      return e;
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        Preconditions
            .checkArgument(argv.length >= 1 && argv.length <= 4, USAGE);

        String regex = ".*"; // default to accepting all
        if (argv.length >= 2) {
          regex = argv[1];
        }
        boolean startFromEnd = false;
        if (argv.length >= 3) {
          startFromEnd = Boolean.parseBoolean(argv[2]);
        }
        int recurseDepth = 0;
        if (argv.length >= 4) {
          recurseDepth = Integer.parseInt(argv[3]);
          Preconditions.checkArgument(recurseDepth >= 0,
              "\"recurseDepth\" should be >= 0, but was: " + recurseDepth
                  + ".\n" + USAGE);
        }

        // delim regex, delim mode
        Pair<String, DelimMode> mode = TailSource.extractDelimContext(ctx);
        if (mode != null) {
          return new TailDirSource(new File(argv[0]), regex, startFromEnd,
              recurseDepth, mode.getLeft(), mode.getRight());
        }
        return new TailDirSource(new File(argv[0]), regex, startFromEnd,
            recurseDepth);

      }
    };
  }
  
  public static SourceBuilder checkPointBuilder() {
	  return new SourceBuilder() {
		  @Override
	      public EventSource build(Context ctx, String... argv) {
	        Preconditions
	            .checkArgument(argv.length >= 1 && argv.length <= 4, USAGE);

	        String regex = ".*"; // default to accepting all
	        if (argv.length >= 2) {
	          regex = argv[1];
	        }
	        boolean startFromEnd = false;
	        if (argv.length >= 3) {
	          startFromEnd = Boolean.parseBoolean(argv[2]);
	        }
	        int recurseDepth = 0;
	        if (argv.length >= 4) {
	          recurseDepth = Integer.parseInt(argv[3]);
	          Preconditions.checkArgument(recurseDepth >= 0,
	              "\"recurseDepth\" should be >= 0, but was: " + recurseDepth
	                  + ".\n" + USAGE);
	        }

	        // delim regex, delim mode
	        Pair<String, DelimMode> mode = TailSource.extractDelimContext(ctx);
	        TailDirSource source = null;
	        if (mode != null) {
	        	source = new TailDirSource(new File(argv[0]), regex, startFromEnd,
	  	              recurseDepth, mode.getLeft(), mode.getRight()); 
	        } else {
	        	source = new TailDirSource(new File(argv[0]), regex, startFromEnd,
	    	            recurseDepth);
	        }
	        
	        String logicalNodeName = ctx.getValue(LogicalNodeContext.C_LOGICAL);
	        Preconditions.checkArgument(logicalNodeName != null,
            "Context does not have a logical node name");
	        source.initCheckPoint(logicalNodeName);
	        return source;
	      }
	  };
  }

  public void setCheckPointManager(CheckPointManager checkpointManager) {
	this.checkPointManager = checkpointManager;
  }
}
