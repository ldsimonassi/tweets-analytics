package tweets.partition.spouts;

import java.util.List;
import java.util.Map;

import tweets.partition.utils.PartitionedRQ;
import tweets.simple.spouts.TransactionMetadata;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetsPartitionedTransactionalSpout extends BasePartitionedTransactionalSpout<TransactionMetadata> {
	private static final long serialVersionUID = 1L;
	public static final int MAX_TRANSACTION_SIZE = 30;

	public static class TweetsPartitionedTransactionalCoordinator implements Coordinator {
    		@Override    
    		public int numPartitions() {
        		return 4;
        }

        @Override
        public boolean isReady() {
        		return true;
        }
                
        @Override
        public void close() {
        }
    }
    
    public static class TweetsPartitionedTransactionalEmitter implements Emitter<TransactionMetadata> {
    		PartitionedRQ rq = new PartitionedRQ();
    		
    		@Override
    		public TransactionMetadata emitPartitionBatchNew(TransactionAttempt tx, 
    														    BatchOutputCollector collector, 
    														    int partition, 
    														    TransactionMetadata lastPartitionMeta) {
    			long nextRead;
    			
    			if(lastPartitionMeta == null)
    				nextRead = rq.getNextRead(partition);
    			else {
    				nextRead = lastPartitionMeta.from + lastPartitionMeta.quantity;
    				rq.setNextRead(partition, nextRead); // Move the cursor
    			}
    			
    			long quantity = rq.getAvailableToRead(partition, nextRead);
    			quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
    			TransactionMetadata metadata = new TransactionMetadata(nextRead, (int)quantity);
    			
    			emitPartitionBatch(tx, collector, partition, metadata);
    			return metadata;
    		}
    								
    		@Override
    		public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, TransactionMetadata partitionMeta) {
    			if(partitionMeta.quantity <= 0)
    				return ;
    			
    			List<String> messages = rq.getMessages(partition, partitionMeta.from, partitionMeta.quantity);
    			long tweetId = partitionMeta.from;
    			for (String msg : messages) {
    				collector.emit(new Values(tx, ""+tweetId, msg));
    				tweetId ++;
			}
        }
    		
    		@Override
        public void close() {
        }
    }

	@SuppressWarnings("rawtypes")
	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		return new TweetsPartitionedTransactionalCoordinator();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Emitter<TransactionMetadata> getEmitter(Map conf, TopologyContext context) {
		return new TweetsPartitionedTransactionalEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid", "tweet_id", "tweet"));
	}
}
