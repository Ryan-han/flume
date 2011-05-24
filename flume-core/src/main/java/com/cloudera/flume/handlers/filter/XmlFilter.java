package com.cloudera.flume.handlers.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.text.TailSource;
import com.google.common.base.Preconditions;

public class XmlFilter extends EventSinkDecorator<EventSink> {
	Log LOG = LogFactory.getLog(XmlFilter.class);
	private SAXParser sax;
	private String logicalNodeName;
	
	public static final String LOG_ID = "LogId";

	public XmlFilter(EventSink s, String logicalNodeName) {
		super(s);
		this.logicalNodeName = logicalNodeName;
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		if(e.get(AckChecksumInjector.ATTR_ACK_TYPE) != null) {
			if(!Arrays.equals(e.get(AckChecksumInjector.ATTR_ACK_TYPE), AckChecksumInjector.CHECKSUM_MSG)) {
				super.append(e);
				return;
			}
		}
		
		ByteArrayInputStream bis = new ByteArrayInputStream(e.getBody());
		try {
			sax.parse(bis, new TransactionXmlHandler(e));
		} catch (SAXException e1) {
			e1.printStackTrace();
		}
		
		// Log의 UUID 생성
		CRC32 crc = new CRC32();
		crc.reset();
		crc.update(e.getBody());
		long hash = crc.getValue();
		e.set(LOG_ID, (logicalNodeName + new String(e.get(TailSource.A_TAILSRCFILE)) + hash).getBytes());
		super.append(e);
	}

	static final String SYSTEM_HEADER = "SystemHeader";
	static final String DATA_HEADER = "DataHeader";
	static final String BODY = "Body";
	static final String TRANSACTION_LOG = "TransactionLog";

	class TransactionXmlHandler extends DefaultHandler {
		String currentTag;
		String parentTag;
		String text;
		Event e;

		TransactionXmlHandler(Event e) {
			this.e = e;
		}

		@Override
		public void startDocument() throws SAXException {
			super.startDocument();
		}

		@Override
		public void endDocument() throws SAXException {
			super.endDocument();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if (qName.equals(SYSTEM_HEADER) || qName.equals(DATA_HEADER)
					|| qName.equals(BODY)) {
				parentTag = qName;
			} else {
				currentTag = qName;
				text = null;
			}
			super.startElement(uri, localName, qName, attributes);
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			super.endElement(uri, localName, qName);
			if (qName.equals(SYSTEM_HEADER) || qName.equals(DATA_HEADER)
					|| qName.equals(BODY) || qName.equals(TRANSACTION_LOG)) {
			} else {
				if(text != null) {
					e.set(parentTag + "." + currentTag, text.getBytes());
				} 
			}

		}

		@Override
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			text = new String(ch, start, length).trim();
			super.characters(ch, start, length);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		LOG.info("init sax parser");
		try {
			sax = SAXParserFactory.newInstance().newSAXParser();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		super.open();
	}

	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			@Override
			public EventSinkDecorator<EventSink> build(Context context,
					String... argv) {
				Preconditions.checkArgument(argv.length == 0, "usage: xmlFilter");
				Preconditions.checkNotNull(context.getValue(LogicalNodeContext.C_LOGICAL));
				return new XmlFilter(null, context.getValue(LogicalNodeContext.C_LOGICAL));
			}
		};
	}

}
