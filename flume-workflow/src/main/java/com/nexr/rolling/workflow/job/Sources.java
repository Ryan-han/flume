package com.nexr.rolling.workflow.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author dani.kim@nexr.com
 */
public class Sources {
	private Map<String, List<Source>> sourceMap;

	public Sources() {
		sourceMap = new HashMap<String, List<Source>>();
	}
	
	public void addSource(String group, Source source) {
		if (!sourceMap.containsKey(group)) {
			sourceMap.put(group, new ArrayList<Sources.Source>());
		}
		sourceMap.get(group).add(source);
	}
	
	public void addSource(Source source) {
		addSource(source.group, source);
	}
	
	public Collection<String> keys() {
		return sourceMap.keySet();
	}
	
	public List<Source> get(String key) {
		return sourceMap.get(key);
	}
	
	public static class Source {
		@JsonProperty
		String path;
		@JsonProperty
		String type;
		@JsonProperty
		String group;
		int fileCount;
		FileStatus[] partials;
		
		public Source() {
		}
		
		public Source(String type, String group, FileStatus[] partials) {
			this.path = String.format("%s/%s", type, group);
			this.type = type;
			this.group = group;
			this.partials = partials;
			this.fileCount= partials.length;
		}
		
		public void setPartials(FileStatus[] partials) {
			this.partials = partials;
			this.fileCount = partials.length;
		}
		
		public static class JsonSerializer {
			public static String serialize(Source source) {
				try {
					return new ObjectMapper().writeValueAsString(source);
				} catch (Exception e) {
				}
				return null;
			}
		}
		
		public static class JsonDeserializer {
			public static Source deserialize(String json) {
				try {
					return new ObjectMapper().readValue(json, Source.class);
				} catch (Exception e) {
				}
				return null;
			}
		}
	}
}
