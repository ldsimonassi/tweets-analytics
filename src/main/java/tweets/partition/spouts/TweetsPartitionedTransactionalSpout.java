package tweets.partition.spouts;

import java.util.List;
import java.util.Map;

import tweets.partition.utils.PartitionedRQ;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetsPartitionedTransactionalSpout extends BasePartitionedTransactionalSpout<PartitionedTransactionMetadata> {
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
    
    public static class TweetsPartitionedTransactionalEmitter implements Emitter<PartitionedTransactionMetadata> {
    		PartitionedRQ rq = new PartitionedRQ();
    		
    		@Override
    		public PartitionedTransactionMetadata emitPartitionBatchNew(TransactionAttempt tx, 
    														    BatchOutputCollector collector, 
    														    int partition, 
    														    PartitionedTransactionMetadata lastPartitionMeta) {
    			long nextRead;
    			
    			if(lastPartitionMeta == null)
    				nextRead = rq.getNextRead(partition);
    			else
    				nextRead = lastPartitionMeta.from + lastPartitionMeta.quantity;
    			
    			long quantity = rq.getAvailableToRead(partition, nextRead);
    			quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
    			PartitionedTransactionMetadata metadata = new PartitionedTransactionMetadata(nextRead, (int)quantity); 
    			
    			if(quantity > 0)
    				emitPartitionBatch(tx, collector, partition, metadata);

    			return metadata;
    		}
    								
    		@Override
    		public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, PartitionedTransactionMetadata partitionMeta) {
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

	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		return new TweetsPartitionedTransactionalCoordinator();
	}

	@Override
	public Emitter<PartitionedTransactionMetadata> getEmitter(Map conf, TopologyContext context) {
		return new TweetsPartitionedTransactionalEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid", "tweet_id", "tweet"));
	}
}
