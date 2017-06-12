package model;
public class Rating {

 
    private String title;
    private Float rating;
    private int year;

    public Rating(){
        this.title = null;
        this.rating = null;
        this.year = 0;
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

    public void setRating(float rating) {
        this.rating = rating;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}