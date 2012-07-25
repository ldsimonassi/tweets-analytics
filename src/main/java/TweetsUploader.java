import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;


public class TweetsUploader {
	static Jedis jedis;
	
	public static void flushRedis() {
		jedis.flushAll();
	}
	
	public static void main(String[] args) throws IOException {
		System.out.println("This process is an infinite loop that uploads a tweets file to the redis server.");
		System.out.println("Press Ctrl-C to finish this process.");

		jedis = new Jedis("localhost");
		flushRedis();
		
		String fileName = (args.length > 1)?args[1]:"./tweets";
		while(true) {
			loadTweetsFile(fileName);
		}
	}

	private static void loadTweetsFile(String fileName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String tweet;

		while((tweet = reader.readLine()) != null) {
			addToRedis(tweet);
		}

		reader.close();
	}

	private static void addToRedis(String tweet) {
		String nextTweet = jedis.get("NEXT_WRITE");
		if(nextTweet == null)
			nextTweet = "0";
		jedis.watch("NEXT_WRITE");
		Transaction transaction = jedis.multi();
		transaction.set(nextTweet, tweet);
		transaction.incr("NEXT_WRITE");
		transaction.exec();
	}
}
