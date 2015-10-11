package mp3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

	public static void main(String[] args) throws Exception {

		/*
		 * ----------------------TODO----------------------- Task: wire up the
		 * topology
		 * 
		 * NOTE:make sure when connecting components together, using the
		 * functions setBolt(name,…) and setSpout(name,…), you use the following
		 * names for each component: FileReaderSpout -> "spout"
		 * SplitSentenceBolt -> "split" WordCountBolt -> "count"
		 * 
		 * 
		 * 
		 * -------------------------------------------------
		 */

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new FileReaderSpout(), 5);
		builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping(
				"spout");
		builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping(
				"split", new Fields("word"));

		Config config = new Config();
		config.setDebug(true);

		config.put("file", args[0]);

		config.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", config, builder.createTopology());
		Thread.sleep(2 * 60 * 1000);
		cluster.shutdown();

	}
}
