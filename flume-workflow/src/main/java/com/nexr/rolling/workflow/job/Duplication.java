package com.nexr.rolling.workflow.job;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author dani.kim@nexr.com
 */
public class Duplication {
	private String source;
	private String destination;
	private String path;
	
	public static class JsonSerializer {
		public static String serialize(Duplication duplication) {
			try {
				return new ObjectMapper().writeValueAsString(duplication);
			} catch (Exception e) {
				return null;
			}
		}
	}
	
	public static class JsonDeserializer {
		public static Duplication deserialize(String json) {
			try {
				return new ObjectMapper().readValue(json, Duplication.class);
			} catch (Exception e) {
			}
			return null;
		}
	}
	
	public Duplication() {
	}

	public Duplication(String source, String destination, String path) {
		this.source = source;
		this.destination = destination;
		this.path = path;
	}
	
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
