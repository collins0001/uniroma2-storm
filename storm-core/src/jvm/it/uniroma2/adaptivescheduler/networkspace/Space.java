package it.uniroma2.adaptivescheduler.networkspace;

public interface Space {
	
	public Point difference(Point a, Point b);
	
	public double norm(Point a);
	
	public double distance(Point a, Point b);
	
	public Point multiply(double a, Point p);

}
