package com.detica.cyberreveal.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A storm spout which reads a file and outputs each line to a spearate tuple.
 */
public class BookLineSpout extends BaseRichSpout {

	private static final long serialVersionUID = -7281111950770566776L;
	private SpoutOutputCollector collector;
	private List<String> lines;

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context, final SpoutOutputCollector spoutCollector) {
		this.lines = new ArrayList<String>();
		this.collector = spoutCollector;
		// Read input file, one line at a time, and add each line to a list
		File inputFile = new File((String) conf.get("inputFile"));
		try {
			FileReader inStream = new FileReader(inputFile);
			try {
				BufferedReader buff = new BufferedReader(inStream);
				try {
					String line = buff.readLine();
					while (line != null) {
						this.lines.add(line);
						line = buff.readLine();
					}
				} finally {
					buff.close();
				}
			} finally {
				inStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		if (!this.lines.isEmpty()) {
			String line = this.lines.remove(0);
			this.collector.emit(new Values(line));
		}
	}

	@Override
	public void ack(final Object id) {
		// Acknowledges that a tuple has successfully been emitted from the spout
		System.out.println("OK:" + id);
	}

	@Override
	public void fail(final Object id) {
		// Acknowledges that a tuple has failed to be emitted from the spout
		System.out.println("FAIL:" + id);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
