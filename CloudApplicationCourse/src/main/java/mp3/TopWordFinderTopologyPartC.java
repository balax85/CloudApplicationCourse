package mp3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file, splits the senteces into words, normalizes the
 * words such that all words are lower case and common words are removed, and
 * then count the number of words.
 */
public class TopWordFinderTopologyPartC {

	public static void main(String[] args) throws Exception {

		/*
		 * ----------------------TODO----------------------- Task: wire up the
		 * topology
		 * 
		 * NOTE:make sure when connecting components together, using the
		 * functions setBolt(name,…) and setSpout(name,…), you use the following
		 * names for each component:
		 * 
		 * FileReaderSpout -> "spout" SplitSentenceBolt -> "split" WordCountBolt
		 * -> "count" NormalizerBolt -> "normalize"
		 * 
		 * 
		 * 
		 * -------------------------------------------------
		 */

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new FileReaderSpout());
		builder.setBolt("split", new SplitSentenceBolt()).shuffleGrouping(
				"spout");
		builder.setBolt("count", new WordCountBolt()).fieldsGrouping(
				"normalize", new Fields("word"));
		builder.setBolt("normalize", new NormalizerBolt()).shuffleGrouping(
				"count");

		Config config = new Config();
		config.setDebug(true);

		config.put("file", args[0]);

		config.setMaxTaskParallelism(1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", config, builder.createTopology());
		Thread.sleep(2 * 60 * 1000);
		cluster.shutdown();

	}
}
