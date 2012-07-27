package tweets.partition.utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PartitionedTweetsUploader {
	static PartitionedRQ rq;

	public static void main(String[] args) throws IOException {
		System.out.println("This process is an infinite loop that uploads a tweets file to the redis server.");
		System.out.println("Press Ctrl-C to finish this process.");

		rq = new PartitionedRQ();
		rq.flush();
		
		String fileName = (args.length > 1)?args[1]:"./tweets";
		while(true) {
			loadTweetsFile(fileName);
		}
	}

	private static void loadTweetsFile(String fileName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String tweet;
		int i = 0;
		while((tweet = reader.readLine()) != null) {
			rq.addMessage(i%4, tweet);
			i++;
		}
		reader.close();
	}
}
