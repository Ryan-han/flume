package com.nexr.framework.conf;

import java.util.Iterator;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * @author dani.kim@nexr.com
 */
public class Configuration extends org.apache.hadoop.conf.Configuration {
	public void setConfiguration(String configuration) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objectNode = mapper.readValue(configuration, ObjectNode.class);
		Iterator<String> iter = objectNode.getFieldNames();
		while (iter.hasNext()) {
			String name = iter.next();
			set(name, objectNode.get(name).getTextValue());
		}
	}

	public String getConfigurationAsString() {
		Iterator<Entry<String, String>> it = iterator();
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode configuration = new ObjectNode(mapper.getNodeFactory());
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			configuration.put(entry.getKey(), entry.getValue());
		}
		try {
			return mapper.writeValueAsString(configuration);
		} catch (Exception e) {
		}
		return null;
	}
}
