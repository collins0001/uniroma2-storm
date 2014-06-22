package it.uniroma2.adaptivescheduler.networkspace;

import it.uniroma2.adaptivescheduler.common.Point;

import java.util.List;
import java.util.Map;

public interface KNearestNodes {

	public List<KNNItem> getKNearestNode(int k, Point position, Map<String, Node> nodes);
	
}
