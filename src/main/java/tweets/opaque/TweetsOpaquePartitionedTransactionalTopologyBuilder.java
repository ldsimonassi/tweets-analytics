package tweets.opaque;

import org.apache.log4j.Logger;

import tweets.opaque.bolts.RedisOpaqueCommiter;
import tweets.opaque.spouts.TweetsOpaquePartitionedTransactionalSpout;
import tweets.partition.spouts.TweetsPartitionedTransactionalSpout;
import tweets.simple.bolts.HashtagSplitterBolt;
import tweets.simple.bolts.RedisCommiterCommiterBolt;
import tweets.simple.bolts.UserHashtagJoinBolt;
import tweets.simple.bolts.UserSplitterBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

public class TweetsOpaquePartitionedTransactionalTopologyBuilder {

	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().removeAllAppenders();
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new TweetsOpaquePartitionedTransactionalSpout(), 2);
		builder.setBolt("hashtag-splitter", new HashtagSplitterBolt(), 4).shuffleGrouping("spout");
		builder.setBolt("redis-opaque-commiter", new RedisOpaqueCommiter())
					.globalGrouping("hashtag-splitter", "hashtags");

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();
        config.setMaxSpoutPending(1);
        config.setMaxTaskParallelism(20);

        cluster.submitTopology("test-topology", config, builder.buildTopology());

        Thread.sleep(300000);
	}

}
