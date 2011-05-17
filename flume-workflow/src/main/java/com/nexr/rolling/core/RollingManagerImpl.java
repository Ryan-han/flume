package com.nexr.rolling.core;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nexr.rolling.conf.RollingConfiguration;
import com.nexr.rolling.exception.RollingException;
import com.nexr.rolling.schd.RollingScheduler;

public class RollingManagerImpl implements Daemon, RollingManager,
		IZkDataListener, IZkChildListener {

	private static final Logger log = Logger
			.getLogger(RollingManagerImpl.class);
	
	private boolean isMaster = false;
	
	private ApplicationContext context;
	private ZkClient zkClient;
	private RollingConfiguration conf;
	String config = null;
	RollingConfig rollingConfig = null;
	RollingScheduler scheduler = null;
	
	private String hostName = null;
	private String rollingRootPath = null;
	private String rollingConfigPath = null;
	private String rollingMemberPath = null;
	private String rollingMasterPath = null;
	
	private String dedupPostPath = null;
	private String dedupHourlyPath = null;
	private String dedupDailyPath = null;
	
	@Override
	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
		// TODO Auto-generated method stub
		conf = new RollingConfiguration();
		rollingRootPath = conf.getRollingRootPath();
		rollingConfigPath = conf.getRollingRootPath() + conf.getRollingConfigPath();
		rollingMemberPath = conf.getRollingRootPath() + conf.getRollingMemberPath();
		rollingMasterPath = conf.getRollingRootPath() + conf.getRollingMasterPath();
		
		dedupPostPath = conf.getDedupRootPath() + conf.getDedupPostPath();
		dedupHourlyPath = conf.getDedupRootPath() + conf.getDedupHourlyPath();
		dedupDailyPath = conf.getDedupRootPath() + conf.getDedupDailyPath();
		
		
		hostName = getLocalhostName();
		scheduler = new RollingScheduler();
		zkClient = new ZkClient(conf.getZookeeperServers(), 30000,
				conf.getZookeeperSessionTimeout());

		announceNode();
		masterRegister();
		
		zkClient.subscribeChildChanges(rollingMemberPath, this);
		zkClient.subscribeChildChanges(rollingMasterPath, this);
		
		if(!zkClient.exists(conf.getDedupRootPath())){
			zkClient.createPersistent(conf.getDedupRootPath());
			zkClient.createPersistent(dedupPostPath);
			zkClient.createPersistent(dedupHourlyPath);
			zkClient.createPersistent(dedupDailyPath);
		}
		
		//dedup
		zkClient.subscribeChildChanges(dedupPostPath, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds)
					throws Exception {
				// TODO Auto-generated method stub
				log.info("Post Mr Result is Duplicacted");
								
				log.info("Dedup List");
				for(String dedup : currentChilds){
					dedup = dedup.replace(":", "/");
					log.info(dedup);
				}
				
				PostDedupJob postDedupJob;
				for(String dedup : currentChilds){
					postDedupJob = new PostDedupJob(dedup);
					postDedupJob.initDedupMr();
					postDedupJob.movePostDedupMrData();
					postDedupJob.runPostDedupMr();
				}
			}
		});
		
		zkClient.subscribeChildChanges(dedupHourlyPath, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds)
					throws Exception {
				// TODO Auto-generated method stub
				log.info("Hourly Mr Result is Duplicacted");
				log.info("Dedup List");
				for(String dedup : currentChilds){
					dedup = dedup.replace(":", "/");
					log.info(dedup);
				}	
			}
		});
		
		
		zkClient.subscribeChildChanges(dedupDailyPath, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds)
					throws Exception {
				// TODO Auto-generated method stub
				log.info("Daily Mr Result is Duplicacted");
				log.info("Dedup List");
				for(String dedup : currentChilds){
					dedup = dedup.replace(":", "/");
					log.info(dedup);
				}	
			}
		});
		
		try {
			zkClient.getEventLock().lock();
			
			if (zkClient.exists(rollingConfigPath)) {
				zkClient.subscribeDataChanges(rollingConfigPath, this);
				config = this.zkClient.readData(rollingConfigPath);
			}else{
			}

			if (config != null) {
				configToObj(config);
			}
			
			if(isMaster){
				startMaster();
			}

			log.info("Configuration " + rollingConfig.toString());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.zkClient.getEventLock().unlock();
		}
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		// TODO Auto-generated method stub
		if(parentPath.equals(rollingMemberPath)){
			log.info("Change MemberShip " + currentChilds.size());
			for(String member : currentChilds){
				log.info("Memeber " + member);
			}
		}else if(parentPath.equals(rollingMasterPath)){
			log.info("Master Node Change ");
			masterRegister();
			if(isMaster){
				startMaster();
			}
		}
	}

	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		// TODO Auto-generated method stub
		configToObj(data.toString());
		log.info(rollingConfig.toString());
		if(isMaster){
			startMaster();
		}
			
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		// TODO Auto-generated method stub
		log.info("Delete Path " + dataPath);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		context = new ClassPathXmlApplicationContext("spring.xml");
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		zkClient.delete(rollingMemberPath + "/" + hostName);
		if(zkClient.exists(rollingMasterPath + "/" + hostName)){
			zkClient.delete(rollingMasterPath + "/" + hostName);
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
	
	private void announceNode() throws RollingException{
		log.info("announce node  : " + hostName);
		
		if(!zkClient.exists(rollingMemberPath)){
			zkClient.createPersistent(rollingMemberPath);
		}
		
	    String nodePath = rollingMemberPath + "/" + hostName;
	    if (zkClient.exists(nodePath)) {
			zkClient.delete(nodePath);
		}
		zkClient.createEphemeral(nodePath);
	}
	
	
	private void masterRegister(){
		if(!zkClient.exists(rollingMasterPath)){
			zkClient.createPersistent(rollingMasterPath);
		}
		if(zkClient.getChildren(rollingMasterPath).size() == 0){
			zkClient.createEphemeral(rollingMasterPath + "/" + hostName);
			log.info("Master Node : " + hostName);
		}
		
		if(zkClient.getChildren(rollingMasterPath).get(0).startsWith(hostName)){
			log.info("Config : " + zkClient.getChildren(rollingMasterPath).get(0) + " hostName " + hostName);
			isMaster = true;
		}
	}
	
	private void configToObj(String config) {
		rollingConfig = new RollingConfig();

		StringTokenizer st = new StringTokenizer(config, "\n");
		while (st.hasMoreTokens()) {
			String value = st.nextToken();
			if (value.startsWith(RollingConfig.RAW_PATH)) {
				rollingConfig.setRawPath(value.substring(value.indexOf("=")+1,
						value.length()).trim());
			} else if (value.startsWith(RollingConfig.POST_MR_INPUT_PATH)) {
				rollingConfig.setPostMrInputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.POST_MR_OUTPUT_PATH)) {
				rollingConfig.setPostMrOutputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.POST_MR_RESULT_PATH)) {
				rollingConfig.setPostMrResultPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.HOURLY_MR_INPUT_PATH)) {
				rollingConfig.setHourlyMrInputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.HOURLY_MR_OUTPUT_PATH)) {
				rollingConfig.setHourlyMrOutputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.HOURLY_MR_RESULT_PATH)) {
				rollingConfig.setHourlyMrResultPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DAILY_MR_INPUT_PATH)) {
				rollingConfig.setDailyMrInputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DAILY_MR_OUTPUT_PATH)) {
				rollingConfig.setDailyMrOutputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DAILY_MR_RESULT_PATH)) {
				rollingConfig.setDailyMrResultPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.RETRY_COUNT)) {
				rollingConfig.setRetryCount(Integer.parseInt(value.substring(
						value.indexOf("=")+1, value.length()).trim()));
			} else if (value.startsWith(RollingConfig.RETRY_DELAY)) {
				rollingConfig.setRetryDelaytime(Long.parseLong(value.substring(
						value.indexOf("=")+1, value.length()).trim()));
			} else if (value.startsWith(RollingConfig.NOTIFY_EMAIL)) {
				rollingConfig.setNotifyEmail(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.POST_SCHEDULE)) {
				rollingConfig.setPostSchedule(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.HOURLY_SCHEDULE)) {
				rollingConfig.setHourlySchedule(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DAILY_SCHEDULE)) {
				rollingConfig.setDailySchedule(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DEDUP_MR_INPUT_PATH)) {
				rollingConfig.setDedupMrInputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DEDUP_MR_OUTPUT_PATH)) {
				rollingConfig.setDedupMrOutputPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DEDUP_POST_PATH)) {
				rollingConfig.setDedupPostPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DEDUP_HOURLY_PATH)) {
				rollingConfig.setDedupHourlyPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			} else if (value.startsWith(RollingConfig.DEDUP_DAILY_PATH)) {
				rollingConfig.setDedupDailyPath(value.substring(
						value.indexOf("=")+1, value.length()).trim());
			}
		}
	}
	
	private void startMaster(){
		log.info(hostName + " started as master");
		try {
			if(scheduler.getInstance().isStarted()){
				scheduler.restartScheduler();
			}else{
				scheduler.startScheuler();
			}
			
			scheduler.addDailyJobToScheduler(rollingConfig);
			scheduler.addHourlyJobToScheduler(rollingConfig);
			scheduler.addPostJobToScheduler(rollingConfig);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
