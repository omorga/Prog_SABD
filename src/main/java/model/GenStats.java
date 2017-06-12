package model;

public class GenStats {
	
	private Float avg;
	private Float dev;
	
	public GenStats(){
        this.avg = (float) 0;
        this.dev = (float) 0;
    }
	
	public Float getAvg() {
	      	return avg;
		}

	 public void setAvg(Float avg) {
		    this.avg = avg;
		}
	 public Float getDev() {
	      	return dev;
		}

	 public void setDev(Float dev) {
		    this.dev = dev;
		}
}
