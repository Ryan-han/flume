package com.nexr.rolling.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.log4j.Logger;

import com.nexr.rolling.exception.RollingException;
import com.nexr.rolling.schd.RollingScheduler;
import com.nexr.rolling.workflow.ZkClientFactory;
import com.nexr.sdp.Configuration;

public class RollingManagerImpl implements Daemon, RollingManager, IZkDataListener, IZkChildListener {
	private static final Logger log = Logger.getLogger(RollingManagerImpl.class);
	private boolean isMaster = false;
	
	private ZkClient zkClient = ZkClientFactory.getClient();
	private Configuration config = Configuration.getInstance();
	RollingScheduler scheduler = null;
	private String hostName = null;
	
	
	@Override
	public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
		hostName = getLocalhostName();
		scheduler = new RollingScheduler();

		announceNode();
		masterRegister();
		
		zkClient.subscribeChildChanges(config.getZkMembersBase(), this);
		zkClient.subscribeChildChanges(config.getZkMasterBase(), this);
		try {
			zkClient.getEventLock().lock();
			if (zkClient.exists(config.getZkConfigBase())) {
				zkClient.subscribeDataChanges(config.getZkConfigBase(), this);
				config = this.zkClient.readData(config.getZkConfigBase());
			}
			if (isMaster) {
				startMaster();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.zkClient.getEventLock().unlock();
		}
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		if (parentPath.equals(config.getZkMembersBase())) {
			log.info("Change MemberShip " + currentChilds.size());
			for (String member : currentChilds) {
				log.info("Memeber " + member);
			}
		} else if (parentPath.equals(config.getZkMasterBase())) {
			log.info("Master Node Change ");
			masterRegister();
			if (isMaster) {
				startMaster();
			}
		}
	}

	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		if (dataPath.startsWith(config.getZkConfigBase())) {
			config.setConfiguration(new String((byte[]) data));
		}
		if (isMaster) {
			startMaster();
		}
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		log.info("Delete Path " + dataPath);
	}

	@Override
	public void destroy() {
	}

	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() throws Exception {
		zkClient.delete(config.getZkMemberNode(hostName));
		if (zkClient.exists(config.getZkMasterNode(hostName))) {
			zkClient.delete(config.getZkMasterNode(hostName));
		}
		log.info("Rolling Service Stop..");
	}
	
	
	private String getLocalhostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (final UnknownHostException e) {
			throw new RuntimeException("unable to retrieve localhost name");
		}
	}
	
	private void announceNode() throws RollingException {
		log.info("announce node  : " + hostName);
		if (!zkClient.exists(config.getZkMembersBase())){
			zkClient.createPersistent(config.getZkMembersBase());
		}
		
	    String nodePath = config.getZkMemberNode(hostName);
	    if (zkClient.exists(nodePath)) {
			zkClient.delete(nodePath);
		}
		zkClient.createEphemeral(nodePath);
	}
	
	
	private void masterRegister() {
		if (!zkClient.exists(config.getZkMasterBase())) {
			zkClient.createPersistent(config.getZkMasterBase());
		}
		
		if (zkClient.getChildren(config.getZkMasterBase()).size() == 0) {
			zkClient.createEphemeral(config.getZkMasterNode(hostName));
			log.info("Master Node : " + hostName);
		}
		
		if (zkClient.getChildren(config.getZkMasterBase()).get(0).startsWith(hostName)) {
			log.info("Config : " + zkClient.getChildren(config.getZkMasterBase()).get(0) + " hostName " + hostName);
			isMaster = true;
		}
	}
	
	private void startMaster() {
		log.info(hostName + " started as master");
		try {
			if (RollingScheduler.getInstance().isStarted()){
				scheduler.restartScheduler();
			} else {
				scheduler.startScheuler();
			}
			scheduler.addDailyJobToScheduler(config);
			scheduler.addHourlyJobToScheduler(config);
			scheduler.addPostJobToScheduler(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
