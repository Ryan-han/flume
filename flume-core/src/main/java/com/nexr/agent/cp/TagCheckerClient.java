package com.nexr.agent.cp;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.cp.thrift.CheckPointService;
import com.nexr.cp.thrift.CheckPointService.Client;

/**
 * 컬렉터에게 체크포인트 완료 태그를 읽어오는 클래스
 * FlumeMaster를 쓰면 없어져도 될거 같다.
 * @author bitaholic
 *
 */
public class TagCheckerClient {
	static final Logger Log = LoggerFactory.getLogger(TagCheckerClient.class);
	
	private String agentName; //logicalNode Name
	private Client priClient;
	private List<Client> clients;
	
	public TagCheckerClient(String agentName) {
		this.agentName = agentName;
		clients = new ArrayList<Client>();
	}
	
	public void addTClient(String host, int port, int timeout) throws TTransportException {
		TSocket socket = new TSocket(host, port);
		socket.setTimeout(timeout);
		TFramedTransport transport = new TFramedTransport(socket);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		Client client = new CheckPointService.Client(protocol);
		transport.open();
	
		if(priClient != null) {
			priClient = client;
		}
		clients.add(client);
	}
}
