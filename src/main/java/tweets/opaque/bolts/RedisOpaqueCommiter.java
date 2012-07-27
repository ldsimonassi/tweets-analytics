	package tweets.opaque.bolts;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
	
public class RedisOpaqueCommiter extends BaseTransactionalBolt implements ICommitter  {
	private static final long serialVersionUID = -1;
	BatchOutputCollector collector;
	TransactionAttempt id;
	Map<String, Long> delta;
	DB db;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		this.id = id;
		delta = new HashMap<String, Long>();
		db = new DB();
	}

	private void countHash(String hash){
		Long count = delta.get(hash);
		if(count == null)
			delta.put(hash, 1l);
		else
			delta.put(hash, count + 1);
	}
	
	@Override
	public void execute(Tuple tuple) {
		String hashtag = tuple.getStringByField("hashtag");
		if(hashtag != null)
			countHash(hashtag);
	}

	@Override
	public void finishBatch() {
		String transaction = db.getLastTransactionId();
		if(id.getTransactionId().toString().equals(transaction)){
			// In this case ignore the current state.
			HashMap<String, Long> previousState = db.getPrevious();
			HashMap<String, Long> newState  = db.add(previousState, delta);
			db.setCurrent(newState);
		} else {
			HashMap<String, Long> currentState = db.getCurrent(delta.keySet());
			db.setPreviousState(currentState);
			currentState = db.add(currentState, delta);
			db.setCurrent(currentState);
			db.setLastTransactionId(id);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
}
