package it.uniroma2.adaptivescheduler.networkspace;

import java.security.InvalidParameterException;

public class Point {

	private double[] coordinates;

	public Point(int dimensionality) {
		coordinates = new double[dimensionality];
	}

	public Point(double[] initialCoordinates) {

		if (initialCoordinates == null) {
			throw new InvalidParameterException("Invalid initialCoordinates parameter");
		} else {
			coordinates = new double[initialCoordinates.length];

			for (int i = 0; i < coordinates.length; i++) {
				coordinates[i] += initialCoordinates[i];
			}
		}

	}

	public Point(Point otherPoint) {

		if (otherPoint == null) {
			throw new InvalidParameterException("Other Point is null");
		} else {
			coordinates = new double[otherPoint.getDimensionality()];

			for (int i = 0; i < coordinates.length; i++) {
				coordinates[i] += otherPoint.get(i);
			}
		}

	}

	public void set(int dimension, double value) {
		if (dimension < coordinates.length) {
			coordinates[dimension] = value;
		}
	}

	public double get(int dimension) {
		if (dimension < coordinates.length) {
			return coordinates[dimension];
		}
		return Double.NaN;
	}

	public int getDimensionality() {
		return coordinates.length;
	}

	// public void add(Point otherVector){
	// if (otherVector.coordinates == null || coordinates.length !=
	// otherVector.coordinates.length){
	// System.out.println("Vectors have different dimensionality");
	// }
	//
	// for (int i = 0; i < coordinates.length; i++)
	// coordinates[i] += otherVector.coordinates[i];
	//
	// for (int i = 0; i < coordinates.length; i++)
	// System.out.println("[" + i + "] " + coordinates[i]);
	//
	// }

	public Point getInverse() {

		Point inverse = new Point(coordinates.length);

		for (int i = 0; i < coordinates.length; i++)
			inverse.set(i, -coordinates[i]);

		return inverse;
	}

	@Override
	public String toString() {
		String output = "(";
		for (int i = 0; i < coordinates.length; i++) {
			output += coordinates[i] + (i == coordinates.length - 1 ? "" : ",");
		}
		output += ")";
		return output;
	}

}
