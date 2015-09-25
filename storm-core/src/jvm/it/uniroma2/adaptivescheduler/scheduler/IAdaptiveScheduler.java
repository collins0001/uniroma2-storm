package it.uniroma2.adaptivescheduler.scheduler;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Created by gab on 15/09/15.
 */
public interface IAdaptiveScheduler {

    public void initialize();

    public void schedule(Topologies topologies, Map<String, GeneralTopologyContext> topologyContexts, Cluster cluster);

}
