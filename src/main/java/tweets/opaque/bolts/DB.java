package tweets.opaque.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import backtype.storm.transactional.TransactionAttempt;

public class DB {
	public static final String LAST_TRANSACTION = "LAST_TRANSACTION";
	public static final String PREVIOUS = "PREVIOUS";
	Jedis jedis;
	
	public DB() {
		jedis = new Jedis("localhost");
	}

	public String getLastTransactionId() {
		return jedis.get(LAST_TRANSACTION);
	}

	public HashMap<String, Long> getPrevious() {
		Map<String, String> prev = jedis.hgetAll(PREVIOUS);
		HashMap<String, Long> ret = new HashMap<String, Long>();
		Set<String> keys = prev.keySet();
		for (String key : keys) {
			ret.put("")
		}
		return ret;
	}

	public HashMap<String, Long> add(HashMap<String, Long> previousState,
			Map<String, Long> delta) {
		// TODO Auto-generated method stub
		return null;
	}

	public void setCurrent(HashMap<String, Long> newState) {
		// TODO Auto-generated method stub
		
	}

	public void setPreviousState(HashMap<String, Long> currentState) {
		// TODO Auto-generated method stub
		
	}

	public void setLastTransactionId(TransactionAttempt id) {
		// TODO Auto-generated method stub
		
	}

	public HashMap<String, Long> getCurrent(Set<String> keySet) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
