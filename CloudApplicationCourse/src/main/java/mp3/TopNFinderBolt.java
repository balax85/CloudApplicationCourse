package mp3;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Set;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
	private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
	private int N;

	private long intervalToReport = 20;
	private long lastReportTime = System.currentTimeMillis();

	public TopNFinderBolt(int N) {
		this.N = N;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		/*
		 * ----------------------TODO----------------------- Task: keep track of
		 * the top N words
		 * 
		 * 
		 * -------------------------------------------------
		 */

		String word = tuple.getString(0);
		Integer count = tuple.getInteger(1);

		currentTopWords.put(word, count);

		Set<String> words = currentTopWords.keySet();

		if (currentTopWords.size() > 10) {
			String minWord = null;
			for (String value : words) {
				if (minWord == null
						|| (currentTopWords.get(minWord) > currentTopWords
								.get(value))) {
					minWord = value;
				}
			}
			currentTopWords.remove(minWord);
		}

		// reports the top N words periodically
		if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
			collector.emit(new Values(printMap()));
			lastReportTime = System.currentTimeMillis();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("top-N"));

	}

	public String printMap() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("top-words = [ ");
		for (String word : currentTopWords.keySet()) {
			stringBuilder.append("(" + word + " , " + currentTopWords.get(word)
					+ ") , ");
		}
		int lastCommaIndex = stringBuilder.lastIndexOf(",");
		stringBuilder.deleteCharAt(lastCommaIndex + 1);
		stringBuilder.deleteCharAt(lastCommaIndex);
		stringBuilder.append("]");
		return stringBuilder.toString();

	}
}
