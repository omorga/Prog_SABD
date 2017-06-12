package model;
public class Genres{
	
    private String[] name;
    private Float rating;

    public Genres(){
        this.name = null;
        this.rating = null;
    }
    
    public String[] getName() {
        return name;
    }

    public void setName(String[] title) {
        this.name = title;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
}