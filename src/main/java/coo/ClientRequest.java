package coo;

public class ClientRequest {
	private long id;
	private String[] hashTags;
	private String message;
    private double latitude;
    public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String[] getHashTags() {
		return hashTags;
	}
	public void setHashTags(String[] hashTags) {
		this.hashTags = hashTags;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	private double longitude;
	public ClientRequest(long id, String[] hashTags, String message,
			double latitude, double longitude) {
		super();
		this.id = id;
		this.hashTags = hashTags;
		this.message = message;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	
}
