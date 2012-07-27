package tweets.simple.bolts;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;


public class RedisCommiterCommiterBolt extends BaseTransactionalBolt implements ICommitter {
	private static final long serialVersionUID = 1L;
	public static final String LAST_COMMITED_TRANSACTION_FIELD = "LAST_COMMIT";
	TransactionAttempt id;
	BatchOutputCollector collector;
	Jedis jedis;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		this.id = id;
		this.collector = collector;
		this.jedis = new Jedis("localhost");
	}
	
	HashMap<String, Long> hashtags = new HashMap<String, Long>(); 
	HashMap<String, Long> users = new HashMap<String, Long>();
	HashMap<String, Long> usersHashtags = new HashMap<String, Long>();
	
	private void count(HashMap<String, Long> map, String key, int count) {
		Long value = map.get(key);
		if(value == null) 
			value = (long) 0;
		value += count;
		map.put(key, value);
	}

	@Override
	public void execute(Tuple tuple) {
		String origin = tuple.getSourceComponent();
		if("users-splitter".equals(origin)) {
			String user = tuple.getStringByField("user");
			count(users, user, 1);
		} else if("hashtag-splitter".equals(origin)) {
			String hashtag = tuple.getStringByField("hashtag");
			count(hashtags, hashtag, 1);
		} else if("user-hashtag-merger".equals(origin)) {
			String hashtag = tuple.getStringByField("hashtag");
			String user = tuple.getStringByField("user");
			String key = user + ":" + hashtag;
			Integer count = tuple.getIntegerByField("count");
			count(usersHashtags, key, count);
		}
	}

	
	@Override
	public void finishBatch() {
		String lastCommitedTransaction = null;

		try {
			lastCommitedTransaction = jedis.get(LAST_COMMITED_TRANSACTION_FIELD);
		}catch(Exception e) {
			e.printStackTrace();
		}

		String currentTransaction = ""+id.getTransactionId();

		if(currentTransaction.equals(lastCommitedTransaction))
			return ;

		Transaction multi = jedis.multi();

		multi.set(LAST_COMMITED_TRANSACTION_FIELD, currentTransaction);

		Set<String> keys = hashtags.keySet();

		for (String hashtag : keys) {
			Long count = hashtags.get(hashtag);
			multi.hincrBy("hashtags", hashtag, count);	
		}

		keys = users.keySet();
		for (String user : keys) {
			Long count = users.get(user);
			multi.hincrBy("users", user, count);	
		}

		keys = usersHashtags.keySet();
		for (String key : keys) {
			Long count = usersHashtags.get(key);
			multi.hincrBy("users_hashtags", key, count);	
		}

		multi.exec();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
