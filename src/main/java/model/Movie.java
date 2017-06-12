package model;
public class Movie{

    private String title;
    private Float rating;

    public Movie(){
        this.title = null;
        this.rating = null;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
}