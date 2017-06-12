package model;
import java.io.Serializable;
import java.util.Comparator;

public class ComparatorRating implements Comparator<AvgRating>, Serializable{

	private static final long serialVersionUID = 1L;

	public ComparatorRating(){}
	
	public int compare(AvgRating l1, AvgRating l2) {
		if(l1.getAvg() == l2.getAvg())
			return 0;
		else if(l1.getAvg() > l2.getAvg())
			return -1;
		else 
			return 1;
	}

}