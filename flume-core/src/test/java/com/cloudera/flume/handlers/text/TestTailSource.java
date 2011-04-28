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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;

/**
 * This tests the functionality of tail source using the EventSource api.
 */
public class TestTailSource {
  public static Logger LOG = Logger.getLogger(TestTailSource.class);

  @Before
  public void setDebug() {
    Logger.getLogger(TestTailSource.class).setLevel(Level.DEBUG);
    Logger.getLogger(TailSource.class).setLevel(Level.DEBUG);
  }

  void runDriver(final EventSource src, final EventSink snk,
      final CountDownLatch done, final int count) {
    Thread workerThread = new Thread() {

      @Override
      public void run() {
        try {
          LOG.info("opening src");
          src.open();
          LOG.info("opening snk");
          snk.open();

          EventUtil.dumpN(count, src, snk);
          Clock.sleep(500);
          LOG.info("closing src");
          src.close();
          LOG.info("closing snk");
          snk.close();
          done.countDown();
          LOG.info("triggering latch");
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
        }
      }

    };

    workerThread.start();

  }

  @Test
  public void testTailPermissionDenied() throws IOException,
      FlumeSpecException, InterruptedException {
    File f;
    final EventSource eventSource;
    final CompositeSink eventSink;
    final AtomicBoolean workerFailed;
    Thread workerThread;
    FileWriter writer;
    long sleepTime;
    long eventCount;

    f = File.createTempFile("temp", ".tmp");
    f.setReadable(false, false);

    f.deleteOnExit();

    eventSource = TailSource.builder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath());
    eventSink = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    workerFailed = new AtomicBoolean(false);
    workerThread = new Thread() {

      @Override
      public void run() {
        try {
          eventSource.open();
          eventSink.open();

          EventUtil.dumpN(10, eventSource, eventSink);
          Clock.sleep(500);
          eventSource.close();
          eventSink.close();
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
        }
      }

    };

    workerThread.start();

    writer = new FileWriter(f);

    for (int i = 0; i < 10; i++) {
      writer.append("Line " + i + "\n");
      writer.flush();
    }

    writer.close();

    sleepTime = Math.round(Math.random() * 1000);

    eventCount = ((CounterSink) ReportManager.get().getReportable("count"))
        .getCount();
    assertEquals(0, eventCount);

    LOG.debug("About to sleep for " + sleepTime + " before fixing permissions");
    Thread.sleep(sleepTime);

    f.setReadable(true, false);

    LOG.debug("Permissions fixed. Waiting for eventSource to figure it out");
    workerThread.join();

    assertFalse("Worker thread failed", workerFailed.get());

    eventCount = ((CounterSink) ReportManager.get().getReportable("count"))
        .getCount();
    assertEquals(10, eventCount);
  }

  /**
   * Create a file and write to it.
   */
  @Test
  public void testTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.builder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 30;
    runDriver(src, snk, done, count);
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    assertTrue(done.await(10, TimeUnit.SECONDS));

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());
  }

  /**
   * Create a file and write to it, move it, write another
   */
  @Test
  public void testTailSourceMove() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    File f2 = File.createTempFile("moved", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.builder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 30;
    runDriver(src, snk, done, count);

    // Need to make sure the first file shows up
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count / 2; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    // This sleep is necessary to make sure the file is not moved before tail
    // sees it.
    Clock.sleep(2000);
    f.renameTo(f2);

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    LOG.info("counted " + ctr.getCount());

    FileWriter fw2 = new FileWriter(f);
    for (int i = count / 2; i < count; i++) {
      fw2.append("Line " + i + "\n");
      fw2.flush();
    }
    fw2.close();

    done.await();

    ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());
  }

  /**
   * Create and tail multiple files
   */
  @Test
  public void testMultiTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("multitemp1", ".tmp");
    f.deleteOnExit();
    File f2 = File.createTempFile("multitemp2", ".tmp");
    f2.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.multiTailBuilder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath(),
        f2.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 60;
    runDriver(src, snk, done, count);

    int log1 = 0, log2 = 0;
    FileWriter fw = new FileWriter(f);
    FileWriter fw2 = new FileWriter(f2);
    for (int i = 0; i < count; i++) {
      if (Math.random() > 0.5) {
        fw.append("Line " + i + "\n");
        fw.flush();
        log1++;
      } else {
        fw2.append("Line " + i + "\n");
        fw2.flush();
        log2++;
      }
    }
    fw.close();
    fw2.close();

    assertTrue("Test timed out", done.await(30, TimeUnit.SECONDS));

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    LOG.info("events in file1: " + log1 + " events in file2: " + log2);
    assertEquals(count, ctr.getCount());

  }

  /**
   * Create and tail multiple files and filename.
   */
  @Test
  public void testMultiTailSourceFileName() throws IOException,
      FlumeSpecException, InterruptedException {
    File f = File.createTempFile("multitemp1", ".tmp");
    f.deleteOnExit();
    File f2 = File.createTempFile("multitemp2", ".tmp");
    f2.deleteOnExit();
    final MemorySinkSource snk = new MemorySinkSource();
    final EventSource src = TailSource.multiTailBuilder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath(),
        f2.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 60;
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          snk.open();
          EventUtil.dumpN(count, src, snk);
          src.close();
          snk.close();
          done.countDown();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    t.start();

    int log1 = 0, log2 = 0;
    FileWriter fw = new FileWriter(f);
    FileWriter fw2 = new FileWriter(f2);
    for (int i = 0; i < count; i++) {
      if (Math.random() > 0.5) {
        fw.append("Line " + i + "\n");
        fw.flush();
        log1++;
      } else {
        fw2.append("Line " + i + "\n");
        fw2.flush();
        log2++;
      }
    }
    fw.close();
    fw2.close();

    assertTrue(done.await(15, TimeUnit.SECONDS));

    Event e = null;
    while ((e = snk.next()) != null) {
      byte[] fn = e.get(TailSource.A_TAILSRCFILE);
      String sfn = new String(fn);
      if (!sfn.equals(f.getName()) && !sfn.equals(f2.getName())) {
        Assert.fail("didn't have tail src file metadata! " + sfn + " != "
            + f.getName() + " or " + f2.getName());
      }
    }

  }

  /**
   * This is a tail source that starts from the end of file.
   */
  @Test
  public void testTailSourceStartFromEnd() throws IOException,
      FlumeSpecException, InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();

    // pre-existing file
    final int count = 30;
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    // Test start from end.
    final TailSource src = (TailSource) TailSource.builder().build(
        LogicalNodeContext.testingContext(), f.getAbsolutePath(), "true");
    final CountDownLatch done = new CountDownLatch(1);

    runDriver(src, snk, done, count);

    FileWriter fw2 = new FileWriter(f, true);
    for (int i = 0; i < count; i++) {
      fw2.append("Line " + i + "\n");
      fw2.flush();
    }
    fw2.close();

    assertTrue(done.await(10, TimeUnit.SECONDS));

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());

    Cursor cursor = src.cursors.get(0);
    assertEquals(cursor.lastChannelSize, cursor.lastChannelPos);
  }

  /**
   * This just shows that file output stream truncates existing files
   * 
   * @throws IOException
   */
  @Test
  public void testFileOutputStream() throws IOException {
    File tmp = File.createTempFile("tmp-", ".tmp");
    FileOutputStream f = new FileOutputStream(tmp);
    f.write("0123456789".getBytes());
    f.close();
    assertEquals(10, tmp.length());

    f = new FileOutputStream(tmp);
    f.write("01234".getBytes());
    f.close();
    assertEquals(5, tmp.length());
  }

  /**
   * Regression test for FLUME-218: Ensure cursor is not reset to the beginning
   * of a file if the event rate exceeds a certain level or delays are
   * introduced.
   * 
   * This test is essentially testing a situation where a file gets truncated.
   * In this case there is no move -- only a "reset" of the file. The current
   * implementation is not design to handle this and will not handle it
   * correctly.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Ignore
  @Test
  public void testResetRaceCondition() throws IOException {
    File tmpFile;
    final EventSource source;
    final EventSink sink;
    final AtomicBoolean workerFailed;
    FileOutputStream os;
    Thread thread;

    tmpFile = File.createTempFile("tmp-", ".tmp");
    tmpFile.deleteOnExit();

    source = TailSource.builder().build(LogicalNodeContext.testingContext(),
        tmpFile.getAbsolutePath(), "true");
    sink = CounterSink.builder().build(new ReportTestingContext(), "count");
    workerFailed = new AtomicBoolean(false);
    os = null;

    /*
     * A worker thread that blindly moves events until we send a poison pill
     * message containing "EOF".
     */
    thread = new Thread() {

      @Override
      public void run() {
        try {
          Event e;

          source.open();
          sink.open();

          e = null;

          do {
            e = source.next();
            sink.append(e);
          } while (e != null && !Arrays.equals(e.getBody(), "EOF".getBytes()));

          source.close();
          sink.close();
        } catch (IOException e) {
          LOG.error("Error while reading from / write "
              + "to flume source / sink. Exception follows.", e);
          workerFailed.set(true);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };

    thread.start();

    /*
     * Throw 1000 filler events into our tmp file (purposefully unbuffered) and
     * ensure we get fewer than 50 duplicates.
     */
    try {
      os = new FileOutputStream(tmpFile);

      for (int i = 0; i < 1000; i++) {

        if (i % 100 == 0) {
          os.flush();
          os.close();
          os = new FileOutputStream(tmpFile);
        }

        String s = i + " 1234567890123456789012345678901234567890123456789"
            + "0123456789012334567890\n";
        os.write(s.getBytes());
        Clock.sleep(20);
      }

      os.write("EOF\n".getBytes());
      os.flush();
    } catch (IOException e) {
      LOG.error("Error while writing to tmp tail source file. "
          + "Exception follows.", e);
      Assert.fail();
    } catch (InterruptedException e) {
      LOG.error("Error while writing to tmp tail source file. "
          + "Interrupted during a sleep. Exception follows.", e);
      Assert.fail();
    } finally {
      if (os != null) {
        os.close();
      }
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      Assert.fail("Failed to wait for worker thread to complete - interrupted");
    }

    Assert.assertFalse("Worker thread failed. Check logs for errors.",
        workerFailed.get());

    /*
     * FIXME - These tests should be uncommented when TailSource no longer
     * sleep()s. Currently, this causes a race condition where a file being
     * written to and truncated during a sleep causes a loss of data.
     * 
     * Assert.assertTrue("Saw fewer than 1000 events.", ((CounterSink)
     * sink).getCount() > 1000);
     * Assert.assertTrue("Saw more than 50 dupes for 1000 events",
     * (((CounterSink) sink).getCount() - 1000) < 50);
     */
  }
  
  private File genTempFileWithFixedSize(int lineLength, final long count) throws IOException {
	  File f = File.createTempFile("temp", ".tmp");
	  f.deleteOnExit();

	  StringBuilder sb = new StringBuilder();
	  for(int i=0; i<lineLength-1; i++) {
		  sb.append("a");
	  }
	  sb.append("\n");
	  String line = sb.toString();
	  // pre-existing file
	  FileWriter fw = new FileWriter(f);
	  for (int i = 0; i < count; i++) { // write 5* 30 = 150byte
		  fw.append(sb); 
		  fw.flush();
	  }
	  fw.close();
	  
	  return f;
  }
  
  @Test
  public void testCheckpoint() throws IOException {
	  File f= genTempFileWithFixedSize(5, 30);
	  
	  TailSource src = new TailSource(2000);
	  Cursor c = new Cursor(src.sync, f, 0, f.length(), f.lastModified(), true);
	  src.addCursor(c);

	  //
	  EventSink snk = new CheckpointSink();
	  
	  src.open();
	  try {
		  snk.open();
	  } catch (InterruptedException e1) {
		  e1.printStackTrace();
	  }
	  
	  try {
		  EventUtil.dumpN(31, src, snk);
		  ((CheckpointSink)snk).printCheckPoint();
		  List<Event> cpEventList = ((CheckpointSink)snk).getCheckPointEvent();
		  Assert.assertNotNull(cpEventList);
		  Assert.assertEquals(150l, ByteBuffer.wrap(cpEventList.get(0).get(TailSource.A_TAILSRCOFFSET)).asLongBuffer().get());
		  src.close();
		  snk.close();
	  } catch (InterruptedException e) {
		  e.printStackTrace();
	  }
  }
  
  /**
   * Short.MAX_VALUE * 7 byte 크기 테스트 
   * 
   * Cursor에서 nio를 이용해 파일을 읽을 때 buffer크기가 Short.MAX_VALUE이기 때문에 
   * buffer의 크기 이상의 파일 테스트
   * 예상결과 : buffer가 넘어 갈때마다 checkpoint event가 발생한다.그리고 마지막 파일이 
   * 끝날 때 한개가 발생 Short.MAX_VALUE
   * @throws IOException
   */
  @Test
  public void testCheckpointOverBuffer() throws IOException {
	  final int LINE_LENGTH = 7;
	  
	  File f= genTempFileWithFixedSize(LINE_LENGTH, Short.MAX_VALUE);
	  
	  System.out.println("file length : " + f.length());
	  
	  TailSource src = new TailSource(2000);
	  Cursor c = new Cursor(src.sync, f, 0, f.length(), f.lastModified(), true);
	  src.addCursor(c);

	  //
	  EventSink snk = new CheckpointSink();
	  
	  src.open();
	  try {
		  snk.open();
	  } catch (InterruptedException e1) {
		  e1.printStackTrace();
	  }
	  
	  try {
		  EventUtil.dumpN(Short.MAX_VALUE + LINE_LENGTH,  src, snk);
		  ((CheckpointSink)snk).printCheckPoint();
		  List<Event> cpEventList = ((CheckpointSink)snk).getCheckPointEvent();
		  Assert.assertNotNull(cpEventList);
		  Assert.assertEquals(LINE_LENGTH, cpEventList.size());
		  for(int i=0; i< LINE_LENGTH; i++) {
			  Assert.assertEquals(Short.MAX_VALUE * (i+1), ByteBuffer.wrap(cpEventList.get(i).get(TailSource.A_TAILSRCOFFSET)).asLongBuffer().get());
		  }
		  Assert.assertEquals(0, src.sync.size());// 더이상 읽을 Event가 없는 지 확인 Check no more event in src 
		  src.close();
		  snk.close();
	  } catch (InterruptedException e) {
		  e.printStackTrace();
	  }

  }
  
  static class CheckpointSink extends EventSink.Base {
	  List<Event> checkpointEventList;

	  
	  
	@Override
	public void open() throws IOException, InterruptedException {
		checkpointEventList = new ArrayList<Event>();
		super.open();
	}



	@Override
	public synchronized void append(Event e) throws IOException, InterruptedException {
		if(e.get(TailSource.A_TAILSRCOFFSET) != null) {
			checkpointEventList.add(e);
		} else {
			super.append(e);
		}
	}
	
	public void printCheckPoint() {
		if(checkpointEventList != null && checkpointEventList.size() > 0) {
			for(Event e : checkpointEventList) {
				System.out.println("offset : " + ByteBuffer.wrap(e.get(TailSource.A_TAILSRCOFFSET)).asLongBuffer().get());
			}
		} else {
			System.out.println("no event");
		}
	}
	
	public List<Event> getCheckPointEvent() {
		return this.checkpointEventList;
	}
  }
}
