package tweets.partition.spouts;

public class PartitionedTransactionMetadata {
	
	long from;
	int quantity;
	
	public PartitionedTransactionMetadata(long from, int quantity) {
		this.from = from;
		this.quantity = quantity;
	}
	
	public long getFrom() {
		return from;
	}

	public int getQuantity() {
		return quantity;
	}

	boolean isEmpty() {
		return quantity == 0;
	}
}
