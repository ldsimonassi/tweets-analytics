package tweets.simple;


import org.apache.log4j.Logger;

import tweets.simple.bolts.HashtagSplitterBolt;
import tweets.simple.bolts.RedisCommiterCommiterBolt;
import tweets.simple.bolts.UserHashtagJoinBolt;
import tweets.simple.bolts.UserSplitterBolt;
import tweets.simple.spouts.TweetsTransactionalSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

public class TweetsTransactionalTopologyBuilder {
	public static void main(String[] args) throws InterruptedException {
			Logger.getRootLogger().removeAllAppenders();
			TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new TweetsTransactionalSpout());

			builder.setBolt("users-splitter", new UserSplitterBolt(), 4).shuffleGrouping("spout");
			builder.setBolt("hashtag-splitter", new HashtagSplitterBolt(), 4).shuffleGrouping("spout");

			builder.setBolt("user-hashtag-merger", new UserHashtagJoinBolt(), 4)
						.fieldsGrouping("users-splitter","users", new Fields("tweet_id"))
						.fieldsGrouping("hashtag-splitter", "hashtags", new Fields("tweet_id"));

			builder.setBolt("redis-commiter", new RedisCommiterCommiterBolt())
						.globalGrouping("users-splitter","users")
						.globalGrouping("hashtag-splitter", "hashtags")
						.globalGrouping("user-hashtag-merger");

	        LocalCluster cluster = new LocalCluster();

	        Config config = new Config();
	        config.setMaxSpoutPending(1);
	        config.setMaxTaskParallelism(20);

	        cluster.submitTopology("test-topology", config, builder.buildTopology());

	        Thread.sleep(300000);
		}
}

