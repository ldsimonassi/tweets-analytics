package tweets.simple.spouts;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import tweets.simple.bolts.HashtagSplitterBolt;
import tweets.simple.bolts.RedisCommiterCommiterBolt;
import tweets.simple.bolts.UserHashtagJoinBolt;
import tweets.simple.bolts.UserSplitterBolt;
import tweets.simple.utils.RQ;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class TweetsTransactionalSpout extends BaseTransactionalSpout<TransactionMetadata>{
	
	static final int MAX_TRANSACTION_SIZE = 30;
	
	public static class TweetsTransactionalSpoutCoordinator implements ITransactionalSpout.Coordinator<TransactionMetadata> {
		TransactionMetadata lastTransactionMetadata;
		RQ rq = new RQ();
		long nextRead = 0;

		public TweetsTransactionalSpoutCoordinator() {
			nextRead = rq.getNextRead();
		}

		@Override
		public TransactionMetadata initializeTransaction(BigInteger txid,
				TransactionMetadata prevMetadata) {
			
			long quantity = rq.getAvailableToRead(nextRead);
			quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
			TransactionMetadata ret = new TransactionMetadata(nextRead, (int)quantity);

			nextRead += quantity;
			return ret;
		}

		/**
		 * This method is ALWAYS called before the initializeTransaction.
		 */
		@Override
		public boolean isReady() {
			return rq.getAvailableToRead(nextRead) > 0;
		}

		@Override
		public void close() {
			rq.close();
		}
	}
	
	
	public static class TweetsTransactionalSpoutEmitter implements ITransactionalSpout.Emitter<TransactionMetadata> {

		RQ rq = new RQ();

		public TweetsTransactionalSpoutEmitter() {
		}
		
		@Override
		public void emitBatch(TransactionAttempt tx, TransactionMetadata coordinatorMeta,
				BatchOutputCollector collector) {
			// Acknowledge messages in the queue
			rq.setNextRead(coordinatorMeta.from+coordinatorMeta.quantity);
			// Read the messages specified in the coordinatorMeta
			List<String> messages = rq.getMessages(coordinatorMeta.from, coordinatorMeta.quantity);
			
			long tweetId = coordinatorMeta.from;
			
			// Emit the tuples
			for (String message : messages) {
				if(message==null)
					System.err.println("Warning!: Null message:"+tweetId);
				else
					collector.emit(new Values(tx, ""+tweetId, message));
				tweetId++;
			}
		}

		@Override
		public void cleanupBefore(BigInteger txid) {
		}

		@Override
		public void close() {
			rq.close();
		}
	}

	@Override
	public ITransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(
			Map conf, TopologyContext context) {
		return new TweetsTransactionalSpoutCoordinator();
	}

	@Override
	public backtype.storm.transactional.ITransactionalSpout.Emitter<TransactionMetadata> getEmitter(
			Map conf, TopologyContext context) {
		return new TweetsTransactionalSpoutEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid", "tweet_id", "tweet"));
	}
	
}
