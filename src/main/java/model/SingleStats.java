package model;

public class SingleStats {
	
	private float sum;
	private float sumsq;
	private int count;
	
	public SingleStats(){
		this.sum = 0;
		this.sumsq = 0;
		this.count = 0;
    }

	public float getSumsq() {
		return sumsq;
	}


	public void setSumsq(float sumsq) {
		this.sumsq = sumsq;
	}

	public float getSum() {
		return sum;
	}

	public void setSum(float sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}

