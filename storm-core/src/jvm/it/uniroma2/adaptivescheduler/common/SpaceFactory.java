package it.uniroma2.adaptivescheduler.common;

public class SpaceFactory {
	
	private static boolean useLatencyUtilizationSpace = false;
	
	public static void setUseExtendedSpace(boolean value){
		useLatencyUtilizationSpace = value;
	}
		
	public static Space createSpace(){
		if (useLatencyUtilizationSpace){
			return new LatencyUtilizationSpace();
		}
		return new EuclidianSpace();
	}

}
