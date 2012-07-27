package tweets.simple.spouts;

import java.io.Serializable;


public class TransactionMetadata implements Serializable {
	private static final long serialVersionUID = 1L;

	long from;
	int quantity;
	
	public TransactionMetadata(long from, int quantity) {
		this.from = from;
		this.quantity = quantity;
	}

	@Override
	public String toString() {
		return "TMD (f:"+from+", q:"+quantity+")";
	}
}
