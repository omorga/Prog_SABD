package model;
import java.io.Serializable;
import java.util.TreeSet;

public class Ranking implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int rankMaxSize;
	private TreeSet<AvgRating> avgRating;
	
	public Ranking(){}
	
	public Ranking(int rankMaxSize){
		this.rankMaxSize = rankMaxSize;
		this.setAvgRating(new TreeSet<AvgRating>(new ComparatorRating()));
	}

	public int getRankMaxSize() {
		return rankMaxSize;
	}
	
	public void setRankMaxSize(int rankMaxSize) {
		this.rankMaxSize = rankMaxSize;
	}
	
	public TreeSet<AvgRating> getAvgRating() {
		return avgRating;
	}

	public void setAvgRating(TreeSet<AvgRating> avgRating) {
		this.avgRating = avgRating;
	}
		
	public boolean overMaxSize(){
		if(this.avgRating.size() > this.rankMaxSize)
			return true;
		else 
			return false;
	}

}