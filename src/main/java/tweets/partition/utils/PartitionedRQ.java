package tweets.partition.utils;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class PartitionedRQ {
	public static final String NEXT_READ = "NEXT_READ";
	public static final String NEXT_WRITE = "NEXT_WRITE";

	Jedis jedis;

	public PartitionedRQ() {
		jedis = new Jedis("localhost");
	}

	public long getAvailableToRead(int partition, long current) {
		return getNextWrite(partition) - current;
	}

	public long getNextRead(int partition) {
		String sNextRead = jedis.get(NEXT_READ+partition);
		if(sNextRead == null)
			return 1;
		return Long.valueOf(sNextRead);
	}

	public long getNextWrite(int partition) {
		// This key should always exist, in order for the spout to work properly.
		return Long.valueOf(jedis.get(NEXT_WRITE+partition)); 
	}

	public void close() {
		jedis.disconnect();
	}

	public void setNextRead(int partition, long nextRead) {
		jedis.set(NEXT_READ+partition, ""+nextRead);
	}

	public List<String> getMessages(int partition, long from, int quantity) {
		String[] keys = new String[quantity];

		for (int i = 0; i < quantity; i++)
			keys[i] = partition+":"+(i+from);

		return jedis.mget(keys);
	}
	
	public void addMessage(int partition, String tweet) {
		String nextTweet = jedis.get(NEXT_WRITE+partition);
		if(nextTweet == null)
			nextTweet = "0";
		jedis.watch(NEXT_WRITE+partition);
		Transaction transaction = jedis.multi();
		transaction.set(partition+":"+nextTweet, tweet);
		transaction.incr(NEXT_WRITE+partition);
		transaction.exec();
	}

	public void flush() {
		jedis.flushAll();
	}
}