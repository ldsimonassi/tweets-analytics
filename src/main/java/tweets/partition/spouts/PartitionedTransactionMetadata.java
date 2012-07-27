package tweets.partition.spouts;

import java.io.Serializable;

public class PartitionedTransactionMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
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
