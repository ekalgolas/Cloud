package dht.dhtfs.core;

import java.io.Serializable;

public class GeometryLocation implements Serializable {

	private static final long serialVersionUID = 1L;

	private double locationX;
	private double locationY;

	public GeometryLocation(double x, double y) {
		locationX = x;
		locationY = y;
	}

	public String toString() {
		return "locationX: " + locationX + " locationY: " + locationY;
	}

	public double getLocationX() {
		return locationX;
	}

	public void setLocationX(double locationX) {
		this.locationX = locationX;
	}

	public double getLocationY() {
		return locationY;
	}

	public void setLocationY(double locationY) {
		this.locationY = locationY;
	}

	public double distance(GeometryLocation location) {
		return (locationX - location.getLocationX()) * (locationX - location.getLocationX())
				+ (locationY - location.getLocationY()) * (locationY - location.getLocationY());
	}
}
