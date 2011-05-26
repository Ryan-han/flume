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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.cloudera.flume.agent.DummyCheckPointManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.text.CustomDelimCursor.DelimMode;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.OSUtils;
import com.ibm.icu.util.Calendar;

/**
 * This class tests the tail dir source. It looks at a local file system and
 * tails all the files in the directory. If new files appear in the directory it
 * tails them as well.
 */

public class TestTailDirSource {

  /**
   * This test makes sure we can instantiate one of these without have the
   * specified dir present. This is important because the master may frequently
   * instantiate this, but never open it unless it is supposed to be local.
   */
  @Test
  public void testRemoteDir() {
    // This used to fail by throwing an exn on the master; it should not.
    new TailDirSource(new File("/path/that/does/not/exist"), ".*");
  }

  @Test
  public void testOpenClose() throws IOException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    for (int i = 0; i < 20; i++) {
      src.open();
      src.close();
    }
    FileUtil.rmr(tmpdir);
  }

  @Test
  public void testBuilder() throws IOException, FlumeSpecException {
    File tmpdir = FileUtil.mktempdir();
    String src = "tailDir(\""
        + StringEscapeUtils.escapeJava(tmpdir.getAbsolutePath())
        + "\", \"foo.*\"";

    Context ctx = LogicalNodeContext.testingContext();
    FlumeBuilder.buildSource(ctx, src + ")"); // without startFromEnd param
    FlumeBuilder.buildSource(ctx, src + ", true)"); // with startFromEnd = true
    FlumeBuilder.buildSource(ctx, src + ", false)"); // with startFromEnd =
    // false
    FlumeBuilder.buildSource(ctx, src + ", true,2)"); // recursively w/
    // max-depth 2
    
    FileUtil.rmr(tmpdir);
  }
  
  @Test
  public void testCheckpointBuilder() throws IOException, FlumeSpecException {
	  File tmpdir = FileUtil.mktempdir();
	    String src = "checkpointTailDir(\""
	        + StringEscapeUtils.escapeJava(tmpdir.getAbsolutePath())
	        + "\", \"foo.*\"";

	    Context ctx = LogicalNodeContext.testingContext();
	    FlumeBuilder.buildSource(ctx, src + ")"); // without startFromEnd param
	    FlumeBuilder.buildSource(ctx, src + ", true)"); // with startFromEnd = true
	    FlumeBuilder.buildSource(ctx, src + ", false)"); // with startFromEnd =
	    // false
	    FlumeBuilder.buildSource(ctx, src + ", true,2)"); // recursively w/
	    // max-depth 2
	    
	    String src2 = "checkpointTailDir(\"" 
	    		+ StringEscapeUtils.escapeJava(tmpdir.getAbsolutePath())
	    		+ "\", \"foo.*\", timeLimit=3600)";
	    FlumeBuilder.buildSource(ctx, src2);
	    
	    FileUtil.rmr(tmpdir);  
  }

  @Test(expected = FlumeSpecException.class)
  public void testFailBuilder() throws IOException, FlumeSpecException {
    Context ctx = LogicalNodeContext.testingContext();
    File tmpdir = FileUtil.mktempdir();
    String src = "tailDir(\""
        + StringEscapeUtils.escapeJava(tmpdir.getAbsolutePath())
        + "\", \"\\x.*\")";
    FlumeBuilder.buildSource(ctx, src);
    FileUtil.rmr(tmpdir);
  }
  
  @Test
  public void testTimeLimit() throws IOException, FlumeSpecException, InterruptedException {
  	File tmpdir = FileUtil.mktempdir();

  	File[] oldFiles = new File[2];
  	
  	Calendar beforeOneHour = Calendar.getInstance();
  	beforeOneHour.setTimeInMillis(beforeOneHour.getTimeInMillis() - ( 60 * 60 * 1000) );
  	
  	for(int i=0; i<2; i++) {
  		oldFiles[i] = new File(tmpdir, "oldfile"+i);
  		oldFiles[i].createNewFile();
  		FileWriter out = new FileWriter(oldFiles[i]);
  		out.write("oldfile\n");
  		out.flush();
  		out.close();
  		oldFiles[i].setLastModified(beforeOneHour.getTimeInMillis());
  	}
  	
  	File[] recentFiles = new File[2];
  	for(int i=0; i<2; i++) {
  		recentFiles[i] = new File(tmpdir, "recentfile"+i);
  		recentFiles[i].createNewFile();
  		FileWriter out = new FileWriter(recentFiles[i]);
  		out.write("newfile\n");
  		out.flush();
  		out.close();
  	}
  	
  	String src = "checkpointTailDir(\""+ tmpdir.getAbsolutePath() +"\", timeLimit=1800)";
  	EventSource source = FlumeBuilder.buildSource(LogicalNodeContext.testingContext(), src);
  	AccumulatorSink sink = new AccumulatorSink("foo");
  	
  	Driver driver = new DirectDriver(source, sink);
  	driver.start();
  	
  	Clock.sleep(3000);
  	
  	Assert.assertEquals(4, sink.getCount());
  	
  	driver.stop();
  	
  	FileUtil.rmr(tmpdir);
  }

  void genFiles(File tmpdir, String prefix, int files, int lines)
      throws IOException {
    for (int i = 0; i < files; i++) {
      File tmpfile = File.createTempFile(prefix, "bar", tmpdir);
      PrintWriter pw = new PrintWriter(tmpfile);
      for (int j = 0; j < lines; j++) {
        pw.println("this is file " + i + " line " + j);
      }
      pw.close();
    }
  }
  
  List<String> genFilesWithFixedContent(File tmpdir, String prefix, int fileNum, int lines) throws IOException {
	  List<String> fileNames = new ArrayList<String>();
	  for (int i = 0; i < fileNum; i++) {
	      File tmpfile = File.createTempFile(prefix, "bar", tmpdir);
	      fileNames.add(tmpfile.getName());
	      PrintWriter pw = new PrintWriter(tmpfile);
	      for (int j = 0; j < lines; j++) {
	        pw.println("0123456789"); //TODO 정해진 byte의 줄을 만들 수 있게 수정
	      }
	      pw.close();
	      System.out.println(tmpfile.getName() + " : " + tmpfile.length());
	    } 
	  return fileNames;
  }
  
  void genXmlFile(File tmpdir, String prefix, String rootElementName, int lines) throws IOException {
	  List<String> fileNames = new ArrayList<String>();
      File tmpfile = File.createTempFile(prefix, "bar", tmpdir);
      fileNames.add(tmpfile.getName());
      PrintWriter pw = new PrintWriter(tmpfile);
      for (int i = 0; i < lines; i++) {
	      StringBuilder xml = new StringBuilder()
	      	.append("<").append(rootElementName).append(">")
	      	.append("<name>dgkim").append(new Random().nextInt()).append("</name>")
	      	.append("<age>").append(new Random().nextInt(100)).append("</age>")
	      	.append("</").append(rootElementName).append(">");
	      pw.println(xml.toString());
      }
      pw.close();
  }

  /**
   * Setup sources and sinks, start the driver, then have new files appear,
   */
  @Test
  public void testNoneToNewFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);

    genFiles(tmpdir, "foo", 10, 100);

    Clock.sleep(1000);

    assertEquals(1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);
  }
  
  @Test
  public void testDelimitedTextSource() throws Exception {
	  File tmpdir = FileUtil.mktempdir();
	  TailDirSource src = new TailDirSource(tmpdir, ".*", false, 0, "</Peoples>", DelimMode.INCLUDE_PREV);
	  genXmlFile(tmpdir, "xxx", "Peoples", 10);
	  
	  src.open();
	  Event event = src.next();
	  src.close();
	  FileUtil.rmr(tmpdir);
	  Assert.assertTrue(new String(event.getBody()).contains("</Peoples>"));
  }
  
  @Test
  public void testCheckpointFiles() throws IOException, InterruptedException {
	  File tmpdir = FileUtil.mktempdir();
	  TailDirSource src = (TailDirSource) TailDirSource.checkPointBuilder().build(LogicalNodeContext.testingContext(), tmpdir.getAbsolutePath());
//	  TailDirSource src = new TailDirSource(tmpdir, ".*");
	  DummyCheckPointManager dcpManager = new DummyCheckPointManager();
	  
	  //"0123456789\n" 라인이 100줄인 파일 10개를 만든다. 
	  List<String> fileNames = genFilesWithFixedContent(tmpdir, "foo", 10, 100);
	  
	  Map<String, Long> checkPointMap = new HashMap<String, Long>();
	  for(String fileName : fileNames) {
		  checkPointMap.put(fileName, 550l);
	  }
	  dcpManager.setCheckPointMap(checkPointMap);
	  src.setCheckPointManager(dcpManager);
	  src.initCheckPoint("dummy");
	  
	  AccumulatorSink cnt = new AccumulatorSink("tailcount");
	  
	  EventSinkDecorator<EventSink> eventSinkDecorator = new EventSinkDecorator<EventSink>(cnt) {
			@Override
			public void append(Event e) throws IOException, InterruptedException {
				if(e.get(TailSource.A_TAILSRCOFFSET) != null) {
					return;
				}
				super.append(e);
			}
	  };
	  
	  DirectDriver drv = new DirectDriver(src, eventSinkDecorator);
	  
	  drv.start();
	  Clock.sleep(1000);
	  assertEquals(500, cnt.getCount());
	  
	  System.out.println("cntCount : " + cnt.getCount());
	  drv.stop();
	  src.close();
	  eventSinkDecorator.close();
	  FileUtil.rmr(tmpdir);
  }

  /**
   * Setup sources and sinks, start the driver, then have new files appear. The
   * filter should only pick up the foo* files and not the bar* files
   */
  @Test
  public void testNoneToNewFilteredFiles() throws IOException,
      InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, "foo.*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);

    genFiles(tmpdir, "foo", 10, 100);
    genFiles(tmpdir, "bar", 15, 100);

    Clock.sleep(1000);

    assertEquals(1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);
  }

  @Test
  public void testExistingFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);
  }

  /**
   * Same as existing files test but has a directory included (that should be
   * ignored)
   */
  @Test
  public void testExistingDir() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    File subDir = new File(tmpdir, "subdir");
    subDir.mkdirs();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);

    // only did 10 files, ignored the dir.
    assertEquals(Long.valueOf(10),
        src.getMetrics().getLongMetric(TailDirSource.A_FILESADDED));
  }

  /**
   * Start with existing files, check that the are read, wait a little bit and
   * then add some new files and make sure they are read.
   */
  @Test
  public void testExistingAddFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    // more files show up
    genFiles(tmpdir, "foo", 10, 100);
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);

  }

  @Test
  public void testExistingRemoveFiles() throws IOException,
      InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    FileUtil.rmr(tmpdir);

    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);
  }

  /**
   * This is a tailDir source that starts from the end of files.
   */
  @Test
  public void testTailDirSourceStartFromEnd() throws IOException,
      FlumeSpecException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    // generating files: emulating their existence prior to sink opening
    genFiles(tmpdir, "foo", 10, 100);

    TailDirSource src = new TailDirSource(tmpdir, ".*", true);
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);
    assertEquals(0, cnt.getCount());

    // adding lines to existing files
    addLinesToExistingFiles(tmpdir, 10);

    Clock.sleep(1000);
    assertEquals(10 * 10, cnt.getCount());

    // generating new files
    genFiles(tmpdir, "bar", 10, 100);

    Clock.sleep(1000);
    assertEquals(10 * 10 + 1000, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);

    // in total 20 files were added
    assertEquals(Long.valueOf(20),
        src.getMetrics().getLongMetric(TailDirSource.A_FILESADDED));
  }

  /**
   * This is a tailDir source that tails files in subdirs with max-depth 2.
   */
  @Test
  public void testTailDirsRecursively() throws IOException, FlumeSpecException,
      InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    // generating files: emulating their existence prior to sink opening
    genFiles(tmpdir, "2tail-old", 10, 100);

    // creating subdirs with data, i.e. structure is:
    // root
    // `-- subdir-level1
    // `-- subdir-level2
    // `-- subdir-level3
    // NOTE: the deepest subdir-level3 shouldn't be watched as recurse-depth is
    // 2.
    File subDirL1 = new File(tmpdir, "subdir-level1");
    subDirL1.mkdirs();
    genFiles(subDirL1, "2tail-old1", 10, 10);
    File subDirL2 = new File(subDirL1, "subdir-level2");
    subDirL2.mkdirs();
    genFiles(subDirL2, "2tail-old2", 10, 20);
    File subDirL3 = new File(subDirL2, "subdir-level3");
    subDirL3.mkdirs();
    genFiles(subDirL3, "2tail-old3", 10, 30);

    TailDirSource src = new TailDirSource(tmpdir, ".(2tail)?.*", true, 2);
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);
    assertEquals(0, cnt.getCount());

    // adding lines to existing files
    addLinesToExistingFiles(tmpdir, 10);
    addLinesToExistingFiles(subDirL1, 20);
    addLinesToExistingFiles(subDirL2, 30);
    addLinesToExistingFiles(subDirL3, 40);

    Clock.sleep(1000);
    int expEventsCount = 10 * 10 + 10 * 20 + 10 * 30;
    assertEquals(expEventsCount, cnt.getCount());

    // generating new files
    genFiles(tmpdir, "2tail-new", 10, 40);
    genFiles(subDirL1, "2tail-new1", 10, 30);
    genFiles(subDirL2, "2tail-new2", 10, 20);
    genFiles(subDirL3, "2tail-new3", 10, 10);

    Clock.sleep(1000);
    expEventsCount += 10 * 40 + 10 * 30 + 10 * 20;
    assertEquals(expEventsCount, cnt.getCount());

    // creating new subdir on level 2
    File newSubDirL2 = new File(subDirL1, "subdir-level2-new");
    newSubDirL2.mkdirs();
    genFiles(newSubDirL2, "2tail-new2-new", 10, 100);

    Clock.sleep(1000);
    expEventsCount += 10 * 100;
    assertEquals(expEventsCount, cnt.getCount());

    // deleting some dirs
    FileUtil.rmr(subDirL3);
    FileUtil.rmr(subDirL2);

    Clock.sleep(1000);
    assertEquals(expEventsCount, cnt.getCount());

    // adding back deleted dir, checking that it is caught up
    subDirL2.mkdirs();
    genFiles(subDirL2, "2tail-new2", 10, 100);
    Clock.sleep(1000);
    expEventsCount += 10 * 100;
    assertEquals(expEventsCount, cnt.getCount());

    drv.stop();
    FileUtil.rmr(tmpdir);

    ReportEvent report = src.getMetrics();
    assertEquals(Long.valueOf(80),
        report.getLongMetric(TailDirSource.A_FILESADDED));
    assertEquals(Long.valueOf(20),
        report.getLongMetric(TailDirSource.A_FILESDELETED));
    assertEquals(Long.valueOf(4),
        report.getLongMetric(TailDirSource.A_SUBDIRSADDED));
    assertEquals(Long.valueOf(1),
        report.getLongMetric(TailDirSource.A_SUBDIRSDELETED));
  }

  private void addLinesToExistingFiles(File tmpdir, int lines)
      throws IOException {
    int fileIndex = 0;
    for (File tmpfile : tmpdir.listFiles()) {
      if (tmpfile.isDirectory()) {
        continue;
      }
      PrintWriter pw = new PrintWriter(new FileWriter(tmpfile, true));
      for (int j = 0; j < lines; j++) {
        pw.println("this is file " + (++fileIndex) + " line " + j);
      }
      pw.close();
    }
  }

  /**
   * This creates many files that need to show up and then get deleted. This
   * just verifies that the files have been removed.
   */
  @Test
  public void testCursorExhaustion() throws IOException, InterruptedException {
    // Windows semantics for rm different from unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();

    // This blows up when there are ~2000 files
    genFiles(tmpdir, "foo", 200, 10);
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    ReportEvent rpt1 = src.getMetrics();
    assertEquals(Long.valueOf(200),
        rpt1.getLongMetric(TailDirSource.A_FILESPRESENT));

    FileUtil.rmr(tmpdir); // This fails in windows because taildir keeps file
    // open
    tmpdir.mkdirs();
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    ReportEvent rpt = src.getMetrics();
    assertEquals(rpt.getLongMetric(TailDirSource.A_FILESADDED),
        rpt.getLongMetric(TailDirSource.A_FILESDELETED));
    assertEquals(Long.valueOf(0),
        rpt.getLongMetric(TailDirSource.A_FILESPRESENT));

    drv.stop();
    FileUtil.rmr(tmpdir);
  }
}
