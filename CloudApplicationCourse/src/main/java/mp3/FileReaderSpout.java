package mp3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
	private SpoutOutputCollector _collector;
	private TopologyContext context;

	private BufferedReader br;

	private boolean completed = false;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		/*
		 * ----------------------TODO----------------------- Task: initialize
		 * the file reader
		 * 
		 * 
		 * -------------------------------------------------
		 */
		try {
			FileReader fr = new FileReader(conf.get("file").toString());
			this.br = new BufferedReader(fr);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		this.context = context;
		this._collector = collector;
	}

	@Override
	public void nextTuple() {

		/*
		 * ----------------------TODO----------------------- Task: 1. read the
		 * next line and emit a tuple for it 2. don't forget to sleep when the
		 * file is entirely read to prevent a busy-loop
		 * 
		 * -------------------------------------------------
		 */

		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
		String str;
		try {
			while ((str = this.br.readLine()) != null) {
				if (!str.trim().isEmpty())
					this._collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading typle", e);
		} finally {
			completed = true;
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("word"));

	}

	@Override
	public void close() {
		/*
		 * ----------------------TODO----------------------- Task: close the
		 * file
		 * 
		 * 
		 * -------------------------------------------------
		 */

		try {
			this.br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
