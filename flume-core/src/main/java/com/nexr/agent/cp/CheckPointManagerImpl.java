package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.util.Clock;
import com.nexr.collector.cp.CheckPointHandler;
import com.nexr.cp.thrift.CheckPointService;

public class CheckPointManagerImpl implements CheckPointManager {
	static final Logger log = LoggerFactory
			.getLogger(CheckPointManagerImpl.class);

	private final static String DATE_FORMAT = "yyyyMMdd-HHmmssSSSZ";
	private final String SEPERATOR = "\t";
	private final String LINE_SEPERATOR = "\n";

	private String checkPointFilePath;

	private Map<String, TTransport> agentTransportMap; // agent-collector
														// TTransport
														// mappingInfo
	private Map<String, CheckPointService.Client> agentClientMap; // agent-collector
																	// Client
																	// mappingInfo
	private Map<String, CollectorInfo> agentCollectorInfo;

	private Map<String, List<PendingQueueModel>> agentTagMap; // agent,
																// list<PendingQueueModel>
	private Map<String, List<WaitingQueueModel>> waitedTagList; // agent,
	// WatingQueueModel

	private Object sync = new Object();

	CheckTagIDThread checkTagIdThread;
	ServerThread serverThread;
	ClientThread clientThread;

	String collectorHost;

	List<String> agentList;

	int timeout = 10 * 1000;

	// for collector
	private final List<String> pendingList;
	private final List<String> completeList;

	CheckPointService.Processor processor;
	TNonblockingServerSocket serverSocket;
	TNonblockingServer.Args arguments;
	TServer server;

	private boolean clientStarted = false;

	public CheckPointManagerImpl() {
		agentList = Collections.synchronizedList(new ArrayList<String>());
		agentTagMap = Collections
				.synchronizedMap(new HashMap<String, List<PendingQueueModel>>());
		waitedTagList = Collections
				.synchronizedMap(new HashMap<String, List<WaitingQueueModel>>());
		checkPointFilePath = FlumeConfiguration.get().getCheckPointFile();
		checkTagIdThread = new CheckTagIDThread();
		pendingList = Collections.synchronizedList(new ArrayList<String>());
		completeList = Collections.synchronizedList(new ArrayList<String>());
		agentTransportMap = Collections
				.synchronizedMap(new HashMap<String, TTransport>());
		agentClientMap = Collections
				.synchronizedMap(new HashMap<String, CheckPointService.Client>());
		agentCollectorInfo = Collections
				.synchronizedMap(new HashMap<String, CollectorInfo>());
	}
	

	class ClientThread extends Thread {
		volatile boolean done = false;
		long checkTagIdPeriod = FlumeConfiguration.get()
				.getConfigHeartbeatPeriod();
		CountDownLatch stopped = new CountDownLatch(1);

		ClientThread() {
			super("CheckManager Client");
		}

		TSocket socket = null;
		TTransport transport = null;
		TProtocol protocol = null;
		CheckPointService.Client client = null;

		public void run() {
			log.info("Done " + done + " AgentSize " + agentList.size());
			while (!done) {
				synchronized (sync) {
					if (agentClientMap.size() == agentCollectorInfo.size()) {

					} else {
						for (int i = 0; i < agentList.size(); i++) {
							if (!agentClientMap.containsKey(agentList.get(i))) {
								CollectorInfo ci = agentCollectorInfo.get(agentList.get(i));
								socket = new TSocket(ci.getCollectorHost(), ci.getCollectorPort());
								socket.setTimeout(timeout);
								transport = new TFramedTransport(socket);
								protocol = new TBinaryProtocol(transport);
								client = new CheckPointService.Client(protocol);
								log.info("New Client " + agentList.get(i)
										+ " CollectorINFO ["
										+ ci.getCollectorHost() + ":"
										+ ci.getCollectorPort() + "]");
								try {
									transport.open();

									agentTransportMap.put(agentList.get(i), transport);
									agentClientMap.put(agentList.get(i), client);
								} catch (TTransportException e) {
									log.info(agentList.get(i)+ " Connect refuse ");
								}
							}
						}
					}
				}
				try {
					Thread.sleep(checkTagIdPeriod);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			log.info("ClientThread End; ");
			stopped.countDown();
		}
	};

	public void startClient() {
		if(!clientStarted) {
			log.info("Start client threads in CheckpointManager");
			clientThread = new ClientThread();
			clientThread.start();
			checkTagIdThread.start();
			clientStarted = true;
		}
	}

	@Override
	public void stopClient() {
		CountDownLatch stopped = clientThread.stopped;
		clientThread.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			log.error("Problem waiting for livenessManager to stop", e);
		}

		synchronized (sync) {
			agentTransportMap = new HashMap<String, TTransport>();
			agentClientMap = new HashMap<String, CheckPointService.Client>();
			agentTagMap = new HashMap<String, List<PendingQueueModel>>();
			waitedTagList = new HashMap<String, List<WaitingQueueModel>>();
			agentList = new ArrayList<String>();
			agentCollectorInfo = new HashMap<String, CollectorInfo>();
		}
	}

	class ServerThread extends Thread {
		int port = FlumeConfiguration.get().getCheckPointPort();

		ServerThread() {
			super("CheckManager Server");
		}

		ServerThread(int port) {
			this.port = port;
		}

		public void run() {
			try {
				processor = new CheckPointService.Processor(
						new CheckPointHandler());
				serverSocket = new TNonblockingServerSocket(port);
				arguments = new TNonblockingServer.Args(serverSocket);
				arguments.processor(processor);
				server = new TNonblockingServer(arguments);
				server.serve();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	};

	@Override
	public void startServer() {	
		log.info("Start CheckpoinServer : " + FlumeConfiguration.get().getCheckPointPort());
		serverThread = new ServerThread();
		serverThread.start();
	}

	@Override
	public void startServer(int port) {
		log.info("Start Checkpoint Server : " + port);
		serverThread = new ServerThread(port);
		serverThread.start();
	}

	@Override
	public void stopServer() {
		serverThread.stop();
	}

	@Override
	public void setCollectorHost(String host) {
		this.collectorHost = host;
	}

	@Override
	public String getTagId(String agentName, String fileName) {
		// TODO Auto-generated method stub
		DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
		long pid = Thread.currentThread().getId();
		String prefix = agentName + "_" + fileName;
		Date now = new Date(Clock.unixTime());
		long nanos = Clock.nanos();
		String f;
		synchronized (dateFormat) {
			f = dateFormat.format(now);
		}
		String tagId = String.format("%s_%08d.%s.%012d", prefix, pid, f, nanos);

		return tagId;
	}

	@Override
	public Map<String, Long> getOffset(String logicalNodeName) {
		Map<String, Long> result = new HashMap<String, Long>();

		FileReader fileReader;
		BufferedReader reader;

		File ckpointFilePath = new File(checkPointFilePath + File.separator
				+ logicalNodeName + File.separator + "checkpoint");
		try {
			if (!ckpointFilePath.exists()) {
				return null;
			} else {
				fileReader = new FileReader(ckpointFilePath);
				reader = new BufferedReader(fileReader);
				String line = null;
				while ((line = reader.readLine()) != null) {
					result.put(line.substring(0, line.indexOf(SEPERATOR)), Long
							.valueOf(line.substring(line.indexOf(SEPERATOR),
									line.length()).trim()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	class CheckTagIDThread extends Thread {
		volatile boolean done = false;
		long checkTagIdPeriod = FlumeConfiguration.get()
				.getConfigHeartbeatPeriod();
		CountDownLatch stopped = new CountDownLatch(1);

		CheckTagIDThread() {
			super("Check TagID");
		}

		public void run() {

			while (!done) {
				try {
					checkCollectorTagID();
					Clock.sleep(checkTagIdPeriod);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			stopped.countDown();
		}

	};

	@Override
	public void startTagChecker(String agentName, String collectorHost,
			int collectorPort) {
		log.info("StartTagChecker [" + agentName + ", " + collectorHost + ", " + collectorPort + "]");
		startClient();
		synchronized (sync) {
			if (!agentList.contains(agentName)) {
				agentList.add(agentName);
			}

			if (!agentCollectorInfo.containsKey(agentName)) {
				agentCollectorInfo.put(agentName, new CollectorInfo(
						collectorHost, collectorPort));
			}
		}
	}

	@Override
	public void stopTagChecker(String agentName) {
		log.info("StopTagChecker [" + agentName + "]");
		synchronized (sync) {
			agentTransportMap.remove(agentName);
			agentClientMap.remove(agentName);
			agentTagMap.remove(agentName);
			waitedTagList.remove(agentName);
			agentCollectorInfo.remove(agentName);
			for (int i = 0; i < agentList.size(); i++) {
				if (agentList.get(i) == agentName) {
					agentList.remove(i);
				}
			}
		}
	}

	/**
	 * Agent에서 Collector로 보낸 Tag들을 Q에 등록한다.
	 */
	@Override
	public void addPendingQ(String tagId, String agentName, Map<String, Long> tagContent) {
		log.info("addpendingq : " + tagContent.size());
		
		for(String key : tagContent.keySet()) {
			log.info(key + " : " + tagContent.values());
		}
		
		List<PendingQueueModel> tags;
		PendingQueueModel pqm;
		synchronized (sync) {
			if (agentTagMap.containsKey(agentName)) {
				tags = agentTagMap.get(agentName);
				pqm = new PendingQueueModel(tagId, tagContent);
				tags.add(pqm);
				agentTagMap.put(agentName, tags); //TODO 필요없는 코드 인 것 같다. 
			} else {
				tags = new ArrayList<PendingQueueModel>();
				pqm = new PendingQueueModel(tagId, tagContent);
				tags.add(pqm);
				agentTagMap.put(agentName, tags);
			}
			Log.info("add " + agentName + " : " + tagId + " into PendingQ");
		}
	}

	@Override
	public void addCollectorCompleteList(List<String> tagIds) {
		// TODO Auto-generated method stub
		for (int i = 0; i < tagIds.size(); i++) {
			if (!completeList.contains(tagIds.get(i))) {
				completeList.add(tagIds.get(i));
				log.info("Tag " + tagIds.get(i) + " added CompleteList");
			}
		}
		log.info("CompleteList Size " + completeList.size());
	}

	@Override
	public boolean getTagList(String tagId) {
		// TODO Auto-generated method stub
		boolean res = false;
		String v = null;
		log.info("CompleteList " + completeList.size());
		for (int i = 0; i < completeList.size(); i++) {
			if (completeList.get(i).equals(tagId)) {
				v = completeList.get(i);
				res = true;
				completeList.remove(i);
			}
		}
		log.info("CompleteTag " + v + " Result " + tagId + " " + res);
		return res;
	}

	/**
	 * Agent의 pendingQ에 등록한 tag 들을 Collector에 완료가 되었는지 물어보고 
	 * 완료되었으면 목록에서 삭제를 하고 Checkpoint 파일을 갱신한다.
	 */
	public synchronized void checkCollectorTagID() {
		// pendingQueue에 있는 agent의 tagId를 모두 체크 해보고
		// 마지막 true리턴 받은 값을 기억했다가 checkpoint파일에 update한다.
		boolean res = true;

		try {
			for (int i = 0; i < agentList.size(); i++) {
				List<PendingQueueModel> tags = agentTagMap.get(agentList.get(i));

				if (tags != null && agentClientMap.size() > 0) {
					List<PendingQueueModel> tmp = Collections.synchronizedList(new ArrayList<PendingQueueModel>());
					PendingQueueModel currentTagId = null;
					for (int t = 0; t < tags.size(); t++) {
						if (agentClientMap.get(agentList.get(i)) != null) {
							res = agentClientMap.get(agentList.get(i)).checkTagId(tags.get(t).getTagId()); // Thrift 호출
							//TODO Tag하나씩 호출 하는 것 보다 한번에 호출 하는 것이 네트웍 IO가 덜 발생 할 것 같다.
							if (res) {
								// 현재 TagId 저장 후 리스트에서 삭제.
								currentTagId = tags.get(t);
								log.info("Current TagID "+ currentTagId.getTagId());
								tmp.add(tags.get(t));

							} else {
//								updateWaitingTagList(agentList.get(i), tags
//										.get(t).getTagId(), tags.get(t)
//										.getContents());
							}
						}
					}
					//TODO 체크포인트 파일을 합쳐서 적어야 IO 가 덜 발생할 것 같다.
					if (currentTagId != null) {
						updateCheckPointFile(agentList.get(i), currentTagId);
						for (int t = 0; t < tmp.size(); t++) {
							if (agentTagMap.get(agentList.get(i)) != null) {
								agentList.get(i);
								tmp.get(t);
								agentTagMap.get(agentList.get(i)).remove(tmp.get(t));
							}
						}
					}
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public void updateWaitingTagList(String agentName, String tagId,
			Map<String, Long> contents) {
		Set<String> keySet = waitedTagList.keySet();
		Object[] keys = keySet.toArray();
		log.info("key " + keys.length);

		List<String> deleteAgent = new ArrayList<String>();
		for (int i = 0; i < keys.length; i++) {
			for (int tag = 0; tag < waitedTagList.get(keys[i]).size(); tag++) {
				if (waitedTagList.get(keys[i]).get(tag).getTagId() == tagId
						&& waitedTagList.get(keys[i]).get(tag).getWaitedTime() >= FlumeConfiguration
								.get().getCheckPointTimeout()) {
					PendingQueueModel pqm = new PendingQueueModel(tagId,
							waitedTagList.get(keys[i]).get(tag).getContents());
					updateCheckPointFile(keys[i].toString(), pqm);
					deleteAgent.add(keys[i].toString());
				}
			}
		}
		
		for(int i=0; i<deleteAgent.size(); i++){
			log.info("Delete from Waiting Queue " + deleteAgent.get(i));
			waitedTagList.remove(deleteAgent.get(i));
			agentTagMap.remove(deleteAgent.get(i));
		}

		WaitingQueueModel wqm = null;
		List<WaitingQueueModel> list = null;
		if (waitedTagList.containsKey(agentName)) {
			list = waitedTagList.get(agentName);
			// TagId로 체크
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i).getTagId() == tagId) {
					list.get(i)
							.updateWaitedTime(
									FlumeConfiguration.get()
											.getConfigHeartbeatPeriod());
				}
			}
			waitedTagList.put(agentName, list);
		} else {
			list = new ArrayList<WaitingQueueModel>();
			list.add(new WaitingQueueModel(tagId, contents, 0));
			waitedTagList.put(agentName, list);
		}

	}

	public void updateCheckPointFile(String logicalNodeName,
			PendingQueueModel pendingQueueModel) {
		// TODO Auto-generated method stub
		Map<String, Long> res = pendingQueueModel.getContents();
		Set<String> keySet = res.keySet();
		Object[] keys = keySet.toArray();

		File ckpointFilePath = new File(checkPointFilePath + File.separator
				+ logicalNodeName);

		File ckpointFile = new File(checkPointFilePath + File.separator
				+ logicalNodeName + File.separator + "checkpoint");
		Log.info("CheckPoint File Path " + ckpointFile.toString());
		FileReader fileReader;
		BufferedReader reader;
		FileWriter fw;
		BufferedWriter bw = null;
		StringBuilder contents;

		Map<String, String> compareMap = new HashMap<String, String>();

		try {
			if (!ckpointFilePath.exists()) {
				ckpointFilePath.mkdirs();
				ckpointFile.createNewFile();
			}

			log.info("[" + ckpointFile.getPath() + "]"
					+ " Check Point File Size " + ckpointFile.length());

			String line = null;
			if (ckpointFile.length() > 1) {
				contents = new StringBuilder();
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);

				// 현재 체크포인트 파일을 읽어서 메모리에 저장.
				while ((line = reader.readLine()) != null) {
					compareMap.put(
							line.substring(0, line.indexOf(SEPERATOR)).trim(),
							line.substring(line.indexOf(SEPERATOR),
									line.length()).trim());
				}

				// 입력 받은 TagID의 값 입력
				for (int i = 0; i < keys.length; i++) {
					compareMap.put(keys[i].toString(),
							String.valueOf(res.get(keys[i])));
				}

				Set cpSet = compareMap.keySet();
				Object[] cps = cpSet.toArray();
				for (int i = 0; i < cps.length; i++) {
					contents.append(cps[i].toString() + SEPERATOR
							+ compareMap.get(cps[i]) + LINE_SEPERATOR);
				}

				fw = new FileWriter(ckpointFile);
				bw = new BufferedWriter(fw);
				bw.write(contents.toString());
				log.info("content is : " + contents);
//				bw.close();

			} else {
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);
				contents = new StringBuilder();

				for (int i = 0; i < keys.length; i++) {
					contents.append(keys[i].toString() + SEPERATOR
							+ res.get(keys[i]) + LINE_SEPERATOR);
				}
				fw = new FileWriter(ckpointFile);
				bw = new BufferedWriter(fw);
				bw.write(contents.toString());
				log.info("content is : " + contents);
//				bw.close();
				reader.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bw.flush();
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void recover(String logicalNodeName) throws IOException, InterruptedException, RuntimeException,
		FlumeSpecException {
		LogicalNode logicalNode = FlumeNode.getInstance().getLogicalNodeManager().get(logicalNodeName);
		if (logicalNode == null) {
			log.error("Failed recover [" + logicalNodeName + "] is not registed in CheckpointManager");
			return;
		}
		log.info("Closing logicalNode[" + logicalNodeName + "]");
		logicalNode.close();
		log.info("Closed logicalNode[" + logicalNodeName + "]");
		
		log.info("Restarting LogicalNode [" + logicalNodeName + "]");
		logicalNode.restartNode();
		log.info("Finished LogicalNode [" + logicalNodeName + "]");
	}
}
