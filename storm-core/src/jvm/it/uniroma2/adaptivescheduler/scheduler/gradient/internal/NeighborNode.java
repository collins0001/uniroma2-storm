package it.uniroma2.adaptivescheduler.scheduler.gradient.internal;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;
import it.uniroma2.adaptivescheduler.space.Point;

import java.util.List;
import java.util.Map;

/**
 * Created by Gabriele Scolastri on 28/09/15.
 */
public class NeighborNode {

    private List<AugExecutorDetails> executors;
    private String nodeId;
    private Double datarate;
    private Point position;

    public NeighborNode(List<AugExecutorDetails> executorsInNeighborNode, Point position, Double datarate)
    {
        this.executors = executorsInNeighborNode;
        this.datarate = datarate;
        this.position = position;
        this.nodeId = (this.executors != null ? this.executors.get(0).getNodeId() : null);
    }

    public Double getDatarate() {
        return datarate;
    }

    public Point getPosition() {
        return position;
    }

    public void setPosition(Point position) {
        this.position = position;
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<AugExecutorDetails> getExecutors() {
        return executors;
    }
}
