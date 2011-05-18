package com.nexr.rolling.workflow.job;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author dani.kim@nexr.com
 */
public class Duplication {
	private String type;
	private String newSource;
	private String source;
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

	public Duplication(String type, String newSource, String source, String path) {
		this.type = type;
		this.newSource = newSource;
		this.source = source;
		this.path = path;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getNewSource() {
		return newSource;
	}

	public void setNewSource(String newSource) {
		this.newSource = newSource;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
