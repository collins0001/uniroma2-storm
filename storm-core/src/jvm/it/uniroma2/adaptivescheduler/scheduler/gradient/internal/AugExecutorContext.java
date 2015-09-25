package it.uniroma2.adaptivescheduler.scheduler.gradient.internal;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.task.GeneralTopologyContext;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;

import java.util.*;

/**
 * Created by Gabriele Scolastri on 23/10/15.
 */
public class AugExecutorContext {

    private static final String  SYSTEM_COMPONENT_PREFIX = "__";

    private AugExecutorDetails augExecutor;
    List<AugExecutorDetails> neighborExecutors = new ArrayList<AugExecutorDetails>();
    Map<ExecutorDetails, Boolean> isTargetMapping = new HashMap<ExecutorDetails, Boolean>();

    public AugExecutorContext(AugExecutorDetails augExecutor)
    {
        this.augExecutor = augExecutor;
    }

    public AugExecutorDetails getAugExecutor() {
        return augExecutor;
    }

    public List<AugExecutorDetails> getNeighborExecutors() {
        return neighborExecutors;
    }

    public void addNeighbors(TopologyDetails topology,  GeneralTopologyContext context, SchedulerAssignment assignment)
    {
        addNeighbors(extractSourceComponentsId(augExecutor.getComponentId(), context), false, topology, assignment);
        addNeighbors(extractTargetComponentsId(augExecutor.getComponentId(), context), true, topology, assignment);
    }

    public void addNeighbors(List<String> componentIds, boolean isTarget, TopologyDetails topology, SchedulerAssignment assignment)
    {
        Map<ExecutorDetails, String> ex2cmp = topology.getExecutorToComponent();
        String componentId = null;
        AugExecutorDetails neighborExecutor;
        for(ExecutorDetails exec : ex2cmp.keySet())
        {
            componentId = ex2cmp.get(exec);
            if(! componentIds.contains(componentId))
                continue;

            neighborExecutor = new AugExecutorDetails(exec, componentId, assignment.getExecutorToSlot().get(exec));

            this.neighborExecutors.add(neighborExecutor);
            this.isTargetMapping.put(exec, isTarget);
        }
    }

    private List<String> extractSourceComponentsId(String componentId, GeneralTopologyContext context)
    {
        List<String> sourceComponentsId = new ArrayList<String>();

        Map<GlobalStreamId, Grouping> sources = context.getSources(componentId);
        for(GlobalStreamId globalStreamID : sources.keySet()){
            if (globalStreamID!=null && !globalStreamID.get_componentId().startsWith(SYSTEM_COMPONENT_PREFIX))
                sourceComponentsId.add(globalStreamID.get_componentId());
        }

        return sourceComponentsId;
    }

    private List<String> extractTargetComponentsId(String componentId, GeneralTopologyContext context)
    {
        List<String> targetComponentsId = new ArrayList<String>();

        Map<String, Map<String, Grouping>> targets = context.getTargets(componentId);

        for(String streamId : targets.keySet()){
            Set<String> componentsId = targets.get(streamId).keySet();

            if (streamId!=null && streamId.equals("default"))
                targetComponentsId.addAll(componentsId);
        }

        return targetComponentsId;
    }

    public Boolean isTarget(AugExecutorDetails exec)
    {
        return isTargetMapping.get(exec.getExecutor());
    }

    public Boolean isTarget(ExecutorDetails exec)
    {
        return isTargetMapping.get(exec);
    }

}
