package tweets.opaque.spouts;

import java.util.Map;

import tweets.simple.spouts.TransactionMetadata;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;

public class TweetsOpaquePartitionedTransactionalSpout implements IOpaquePartitionedTransactionalSpout<TransactionMetadata>{
	private static final long serialVersionUID = 1L;

	
	public static class TweetsOpaquePartitionedTransactionalSpoutCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
		@Override
		public boolean isReady() {
			return true;
		}
	}
	
	public static class TweetsOpaquePartitionedTransactionalSpoutEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<TransactionMetadata> {
		@Override
		public TransactionMetadata emitPartitionBatch(
				TransactionAttempt tx, BatchOutputCollector collector,
				int partition, TransactionMetadata lastPartitionMeta) {
			
			return null;
		}

		@Override
		public int numPartitions() {
			return 4;
		}

		@Override
		public void close() {
		}
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid", "tweet_id", "tweet"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Emitter<TransactionMetadata> getEmitter(
			Map conf, TopologyContext context) {
		return null;
	}

	@Override
	public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

}
