/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package com.nexr.data.sdp.rolling.hdfs;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.record.Buffer;

import com.cloudera.flume.core.Event;
import com.google.common.base.Preconditions;

public class LogRecord extends LogRecordJT implements Record {
	public LogRecord() {
	}
	
	public LogRecord(Event e) {
		Preconditions.checkNotNull(e);
		this.mapFields = new TreeMap<String, org.apache.hadoop.record.Buffer>();
		
		Map<String, byte[]> attrs = e.getAttrs();
		for(String key : attrs.keySet()) {
			mapFields.put(key, new Buffer(attrs.get(key)));
		}
	}

	public void add(String key, String value) {
		synchronized (this) {
			if (this.mapFields == null) {
				this.mapFields = new TreeMap<String, org.apache.hadoop.record.Buffer>();
			}
		}
		this.mapFields.put(key, new Buffer(value.getBytes()));
	}

	public String[] getFields() {
		return this.mapFields.keySet().toArray(new String[0]);
	}

	public String getValue(String field) {
		if (this.mapFields.containsKey(field)) {
			return new String(this.mapFields.get(field).get());
		} else {
			return null;
		}
	}

	public boolean containsField(String field) {
		return this.mapFields.containsKey(field);
	}

	public void removeValue(String field) {
		if (this.mapFields.containsKey(field)) {
			this.mapFields.remove(field);
		}
	}
}
