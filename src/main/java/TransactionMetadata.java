import java.io.Serializable;


public class TransactionMetadata implements Serializable {
	private static final long serialVersionUID = 1L;

	public int from, quantity;
	
	public int getTo() {
		return from + quantity;
	}
	
	
	@Override
	public String toString() {
		return "From:" + from + " qty:"+ quantity+ " to:"+getTo();
	}
}
