package coo;

public class Query {
	Coordinate longtitude;
	Coordinate latitute;
	public Query(Coordinate longtitude, Coordinate latitute) {
		super();
		this.longtitude = longtitude;
		this.latitute = latitute;
	}
	public Coordinate getLongtitude() {
		return longtitude;
	}
	public void setLongtitude(Coordinate longtitude) {
		this.longtitude = longtitude;
	}
	public Coordinate getLatitute() {
		return latitute;
	}
	public void setLatitute(Coordinate latitute) {
		this.latitute = latitute;
	}
}
