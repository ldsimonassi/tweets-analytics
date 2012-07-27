package tweets.simple.bolts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class UserHashtagJoinBolt extends BaseBatchBolt {
	private static final long serialVersionUID = 1L;

	BatchOutputCollector collector;
	Object id;
	HashMap<String, HashSet<String>> tweetHashtags;
	HashMap<String, HashSet<String>> userTweets;

	
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, Object id) {
		this.collector = collector;
		this.id = id;
		tweetHashtags = new HashMap<String, HashSet<String>>();
		userTweets = new HashMap<String, HashSet<String>>();
	}
	
	@Override
	public void execute(Tuple tuple) {
		String source = tuple.getSourceStreamId();
		String tweetId = tuple.getStringByField("tweet_id");
		
		if("hashtags".equals(source)) {
			String hashtag = tuple.getStringByField("hashtag");
			add(tweetHashtags, tweetId, hashtag);
		} else if("users".equals(source)) {
			String user = tuple.getStringByField("user");
			add(userTweets, user, tweetId);
		} else {
			System.err.println("WTF!!!");
		}
	}

	@Override
	public void finishBatch() {
		
		for (String user : userTweets.keySet()) {
			Set<String> tweets = getUserTweets(user);
			HashMap<String, Integer> hashtagsCounter = new HashMap<String, Integer>();
			for (String tweet : tweets) {
				Set<String> hashtags = getTweetHashtags(tweet);
				if(hashtags != null) {
					for (String hashtag : hashtags) {
						Integer count = hashtagsCounter.get(hashtag);
						if(count == null) 
							count = 0;
						count ++;
						hashtagsCounter.put(hashtag, count);
					}
					
				}
			}
			
			for (String hashtag : hashtagsCounter.keySet()) {
				int count = hashtagsCounter.get(hashtag);
				collector.emit(new Values(id, user, hashtag, count));
			}
		}
	}

	
	/**
	 * Helpers
	 */
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid", "user", "hashtag", "count"));
	}
	
	private void add(HashMap<String, HashSet<String>> bag, String key, String value) {
		HashSet<String> slot = bag.get(key);
		if(slot == null) {
			slot = new HashSet<String>();
			bag.put(key, slot);
		}
		slot.add(value);
	}
	
	private Set<String> getTweetHashtags(String tweet) {
		return tweetHashtags.get(tweet);
	}
	
	private Set<String> getUserTweets(String user) {
		return userTweets.get(user);
	}
}
