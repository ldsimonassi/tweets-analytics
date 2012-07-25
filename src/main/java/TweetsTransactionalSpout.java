import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
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
		Jedis jedis;
		
		TransactionMetadata lastTransactionMetadata;
		
		public TweetsTransactionalSpoutCoordinator() {
			jedis = new Jedis("localhost");
		}
		
		@Override
		public TransactionMetadata initializeTransaction(BigInteger txid,
				TransactionMetadata prevMetadata) {

			TransactionMetadata ret = new TransactionMetadata();
			System.out.println("initializeTransaction(): prevmetadata:" + prevMetadata );
			// First transaction
			if(prevMetadata == null) {
				String sNextRead = jedis.get("NEXT_READ");
				System.out.println("initializeTransaction(): sNextRead:"+sNextRead);
				if(sNextRead == null) 
					sNextRead = "0";
				
				int nextRead = Integer.valueOf(sNextRead);
				ret.from = nextRead;
			} else {
				ret.from = prevMetadata.from + prevMetadata.quantity + 1;
			}
			
			int nextWrite = Integer.valueOf(jedis.get("NEXT_WRITE"));
			
			System.out.println("initializeTransaction(): nextWrite:"+nextWrite);
			
			int availableToRead = (nextWrite-1)-ret.from;
			
			if(availableToRead > MAX_TRANSACTION_SIZE) 
				ret.quantity = MAX_TRANSACTION_SIZE;
			else 
				ret.quantity = availableToRead;
			
			lastTransactionMetadata = ret;
			System.out.println("initializeTransaction(): returning:" + ret);
			return ret;
		}

		@Override
		public boolean isReady() {
			
			System.out.println("isReady(): starting");
			
			String sNextWrite = jedis.get("NEXT_WRITE");
			
			System.out.println("isReady(): nextWrite:"+ sNextWrite);
			
			if(sNextWrite == null) 
				return false;
			
			int nextWrite = Integer.valueOf(sNextWrite);
			
			if(lastTransactionMetadata == null) {
				System.out.println("isReady(): lastTransaction is null");
				
				String sNextRead = jedis.get("NEXT_READ");
				
				System.out.println("isReady(): nextRead:"+sNextRead);

				if(sNextRead == null)
					return true;
				
				int nextRead = Integer.valueOf(sNextRead);
				
				int availableToRead = (nextWrite-1) -nextRead;
				System.out.println("isReady(): availableToRead:"+availableToRead);
				return availableToRead > 0;
			} 
			int availableToRead = (nextWrite-1) - lastTransactionMetadata.getTo(); 
			
			return availableToRead > 0;
		}

		@Override
		public void close() {
			jedis.disconnect();
		}
	}
	
	
	public static class TweetsTransactionalSpoutEmitter implements ITransactionalSpout.Emitter<TransactionMetadata> {
		
		HashMap<BigInteger, TransactionMetadata> transactionsMetadata;
		Jedis jedis;
		
		public TweetsTransactionalSpoutEmitter() {
			transactionsMetadata = new HashMap<BigInteger, TransactionMetadata>();
			jedis = new Jedis("localhost");
		}
		
		@Override
		public void emitBatch(TransactionAttempt tx, TransactionMetadata coordinatorMeta,
				BatchOutputCollector collector) {
			transactionsMetadata.put(tx.getTransactionId(), coordinatorMeta);
			collector.emit(new Values(tx, "dario"));
		}

		@Override
		public void cleanupBefore(BigInteger txid) {
			TransactionMetadata txData = transactionsMetadata.remove(txid);
			if(txData == null)
				System.err.println("WTF!!");
			
			Transaction redisTx= jedis.multi();
			
			
			
			for(int i = txData.from; i < txData.getTo(); i++){
				redisTx.del(""+i);
			}
			redisTx.set("NEXT_READ", ""+(txData.getTo()+1));
			redisTx.exec();
			System.out.println("Emitter: Cleanup done!!");
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
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
		declarer.declare(new Fields("txid", "algo"));
	}
	
	public static void main(String[] args) throws InterruptedException {
		Logger.getRootLogger().removeAllAppenders();
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new TweetsTransactionalSpout(), 3);
		
        LocalCluster cluster = new LocalCluster();
        
        Config config = new Config();
        config.setMaxSpoutPending(3);
        config.setMaxTaskParallelism(20);
        
        cluster.submitTopology("test-topology", config, builder.buildTopology());
        
        Thread.sleep(30000);
	}

}
