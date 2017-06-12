package model;

public class AvgRating {
	private String title;
	private Float avg;
	
	public AvgRating (){
		this.title = null;
		this.avg = null;
	}
	
	public Float getAvg() {
		return avg;
	}
	public void setAvg(Float avg) {
		this.avg = avg;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
}
