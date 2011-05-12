package com.nexr.rolling.cmd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.nexr.rolling.conf.RollingConfiguration;

public class ConfigUpdate {

	private static final Logger log = Logger.getLogger(ConfigUpdate.class);
	
	private final String LINE_SEPERATOR = "\n";
	
	
	private ZkClient zkClient;
	RollingConfiguration conf = new RollingConfiguration();

	File configFile = null;
	StringBuilder contents;
	
	private String rollingRootPath = null;
	private String rollingConfigPath = null;

	public void updateConfig(String config) {
		configFile = new File(config);
		
		rollingRootPath = conf.getRollingRootPath();
		rollingConfigPath = conf.getRollingRootPath() + conf.getRollingConfigPath();
		
		try {
			FileReader fileReader = new FileReader(configFile);
			BufferedReader reader = new BufferedReader(fileReader);
			String line = null;
			contents = new StringBuilder();
			while ((line = reader.readLine()) != null) {
				contents.append(line + LINE_SEPERATOR);
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		zkClient = new ZkClient(conf.getZookeeperServers(), 30000,
				conf.getZookeeperSessionTimeout());
		zkClient.getEventLock().lock();
		if (!zkClient.exists(rollingRootPath)) {
			zkClient.createPersistent(rollingRootPath);
		}
		if (!zkClient.exists(rollingConfigPath)) {
			zkClient.createPersistent(rollingConfigPath, contents.toString());
		} else {
			zkClient.writeData(rollingConfigPath, contents.toString());
		}
		log.info("Configuration " + contents.toString());
	}

	public static void main(String args[]) {

		ConfigUpdate cu = new ConfigUpdate();
		cu.updateConfig(args[0]);
	
	}
}
