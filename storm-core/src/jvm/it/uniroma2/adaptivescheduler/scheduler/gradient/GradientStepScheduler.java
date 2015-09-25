package it.uniroma2.adaptivescheduler.scheduler.gradient;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.scheduler.*;
import backtype.storm.task.GeneralTopologyContext;
import it.uniroma2.adaptivescheduler.entities.Node;
import it.uniroma2.adaptivescheduler.persistence.DatabaseException;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.persistence.entities.Measurement;
import it.uniroma2.adaptivescheduler.scheduler.IAdaptiveScheduler;
import it.uniroma2.adaptivescheduler.scheduler.gradient.internal.*;
import it.uniroma2.adaptivescheduler.space.KNNItem;
import it.uniroma2.adaptivescheduler.space.KNearestNodes;
import it.uniroma2.adaptivescheduler.space.Point;
import it.uniroma2.adaptivescheduler.space.SimpleKNearestNodes;
import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;
import java.util.*;

/**
 * Created by Gabriele Scolastri on 15/09/15.
 */
public class GradientStepScheduler implements IAdaptiveScheduler {

    /*
        Placement class: utility class for memorizing placement info, i.e.
        - Point position: the positioning coordinates
        - Double cost: the local cost of that position, according to the cost function
     */
    private class Placement {
        private Point position;
        private Double cost;

        public Placement(Placement other)
        {
            this.position = other.position.clone();
            this.cost = other.cost;
        }


        public Placement(Point position)
        {
            this.position = position;
            this.cost = Double.MAX_VALUE;
        }

        public Placement(Point position, Double cost)
        {
            this.position = position;
            this.cost = cost;
        }

        public Point getPosition() {
            return position;
        }

        public void setPosition(Point position) {
            this.position = position;
        }

        public Double getCost() {
            return cost;
        }

        public void setCost(Double cost) {
            this.cost = cost;
        }

        public Integer getDimensionality()
        {
            return (position == null ? null : position.getDimensionality());
        }

        @Override
        public Object clone() {

            return new Placement(this.position.clone(), this.cost);
        }

        @Override
        public String toString() {
            return "Placement{" + this.getPosition().toString() + " : " + this.getCost().toString() + "}";
        }
    }


    private static final String TAG = "GRADIENT";
    private static final String LOG_TAG = "[" + TAG + "] ";


    //private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";
    private static final String SYSTEM_COMPONENT_PREFIX = "__";

    private static Integer CONFIG_PLAN_MAX_EXEC_PER_SLOT = 4;
    private static Double CONFIG_ANALYZE_IMPROVEMENT_THRESHOLD = 10.0;
    private static Double CONFIG_PLAN_MIGRATION_THRESHOLD = 10.0;
    private static Double CONFIG_PLAN_MIGRATION_IMPROVEMENT_FRACTION = 0.0;
    private static Integer CONFIG_PLAN_NEARESTNODE_K = 10;

    private static boolean CONFIG_DEBUG = false;

    private static Integer CONFIG_ANALYZE_MAX_RETRIES = 10;
    private static Integer CONFIG_COOLDOWN_ROUNDS = 5;


    private ISupervisor supervisor;
    private String nodeId;
    private SimpleZookeeperClient zkClient;
    private DatabaseManager databaseManager;
    private ComponentMigrationChecker migrationTracker;
    private QoSMonitor networkSpaceManager;

    private CooldownBuffer<String> topologyCooldownBuffer;

    public GradientStepScheduler(ISupervisor supervisor, SimpleZookeeperClient zkClient,
                                 DatabaseManager databaseManager, QoSMonitor networkSpaceManager, Map conf)
    {
        this.supervisor = supervisor;
        this.zkClient = zkClient;
        this.databaseManager = databaseManager;
        this.networkSpaceManager = networkSpaceManager;
        this.migrationTracker = new ComponentMigrationChecker(zkClient);
        readConfig(conf);

        // TODO: after config, initialize and setup structures ?

    }

    private void readConfig(Map conf)
    {
        // TODO: should first check which parameters are needed, then setup them in config and then read here

        Integer iValue;
        Double dValue;
        String sValue;
        Boolean bValue;

        dValue = (Double) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_IMPROVEMENT_TRESHOLD);
        if(dValue != null && dValue > 0.0)
        {
            CONFIG_ANALYZE_IMPROVEMENT_THRESHOLD = dValue;
        }

        iValue = (Integer) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_RETRY_MAX_COUNTER);
        if(iValue != null && iValue > 0 )
        {
            CONFIG_ANALYZE_MAX_RETRIES = iValue;
        }

        iValue = (Integer) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_TOPOLOGY_COOLDOWN);
        if(iValue != null && iValue > 0 )
        {
            CONFIG_COOLDOWN_ROUNDS = iValue;
        }

        dValue = (Double) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_MIGRATION_TRESHOLD);
        if(dValue != null && dValue >= 0.0 )
        {
            CONFIG_PLAN_MIGRATION_THRESHOLD = dValue;
        }

        dValue = (Double) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_MIGRATION_IMPROVEMENT_FRACTION);
        if(dValue != null && dValue >= 0 )
        {
            CONFIG_PLAN_MIGRATION_IMPROVEMENT_FRACTION = (dValue > 1.0 ? 1.0 : dValue);
        }

        iValue = (Integer) conf.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_K_NEAREST_NODE);
        if(iValue != null && iValue > 0 )
        {
            CONFIG_PLAN_NEARESTNODE_K = iValue;
        }

        iValue = (Integer) conf.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_MAX_EXECUTOR_PER_SLOT);
        if(iValue != null && iValue > 0 )
        {
            CONFIG_PLAN_MAX_EXEC_PER_SLOT = iValue;
        }

        bValue = (Boolean) conf.get(Config.ADAPTIVE_SCHEDULER_GRADIENTSTEP_DEBUG);
        if(bValue != null)
        {
            CONFIG_DEBUG = bValue;
        }

        log("[CONFIG] using debugging: " + CONFIG_DEBUG);
        log("[CONFIG] using improvement threshold: " + CONFIG_ANALYZE_IMPROVEMENT_THRESHOLD);
        log("[CONFIG] using migration threshold: " + CONFIG_PLAN_MIGRATION_THRESHOLD);
        log("[CONFIG] using migration improvement fraction: " + CONFIG_PLAN_MIGRATION_IMPROVEMENT_FRACTION);
        log("[CONFIG] using max retries (g. step halving): " + CONFIG_ANALYZE_MAX_RETRIES);
        log("[CONFIG] using cooldown round number: " + CONFIG_COOLDOWN_ROUNDS);
        log("[CONFIG] using nearest node (k): " + CONFIG_PLAN_NEARESTNODE_K);
        log("[CONFIG] using max exec per slot: " + CONFIG_PLAN_MAX_EXEC_PER_SLOT);
    }

    /**
     * Initializes the following structures:
     * - topologyCooldownBuffer:    cooldown manager, used to let changed topologies to cooldown
     * - migrationTracker:          uses zookeeper in order to check and set component migration infos
     * - databaseManager:           database access, for measurement
     */
    public void initialize()
    {
        log("[INIT] initializing scheduler");

        // Set up cooldown buffer for topologies
        // TODO: after config, set correct cooldown default value
        topologyCooldownBuffer = new CooldownBuffer<String>(CONFIG_COOLDOWN_ROUNDS);

        // Initializing migration tracker
        migrationTracker.initialize();

        // Initializing Database Manager
        try {
            databaseManager.initializeOnSupervisor();
        } catch (DatabaseException e) {
            e.printStackTrace();
        } catch (SQLException e){
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simple logging, using stdout. Every message prepends LOG_TAG value (should be: [Gstep]).
     *
     * @param message: message to log
     */
    private void log(String message)
    {
        // TODO: better logging
        System.out.println(LOG_TAG + message);
    }


    /**
     * Main scheduling method, used to check the best positioning for moveable executors among available
     * worker nodes. The main phases are:
     *
     * - search for moveable executors [MONITOR]
     * - for each moveable executor
     *      - search for best placement in the latency (continuous) space [ANALYZE]
     *      - search for nearest-to-best-placement worker node with available slot [PLAN]
     *      - if found node is different from local node, signal migration [EXECUTE]
     *
     * Moreover, it is taken track of migrating components and topology cooldown counters
     *
     * @param topologies: collection of topologies currently active
     * @param topologyContexts: mapping from topology id to topology contexts (contains grouping infos)
     * @param cluster: information about worker nodes and actual executor position within worker nodes & slots
     */
     public void schedule(Topologies topologies, Map<String, GeneralTopologyContext> topologyContexts, Cluster cluster)
    {
        try {


            long start = System.currentTimeMillis();
            log((CONFIG_DEBUG ? "[TIME]" : "") +  " STARTING SCHEDULING (" + start + ").");

            Map<String, SchedulerAssignment> assignments = cluster.getAssignments();

            if(CONFIG_DEBUG)
                log("[TOPOLOGY] Processing " + assignments.keySet().size() + " topologies.");

            // If a topology in cooldown is removed from current node assignment, then the cooldown must be reset,
            // therefore remove all topologies NOT in assigned set.

            topologyCooldownBuffer.removeOtherThan(assignments.keySet());
            // moving cooldown forward
            topologyCooldownBuffer.tick();

            TopologyDetails topology;
            SchedulerAssignment assignment;

            Map<String, Node> knownNodesCoordinates = networkSpaceManager.copyKnownNodesCoordinate();
            Point localPosition = networkSpaceManager.getCoordinates();

            // adaptation is done for each topology
            for (String topologyID : assignments.keySet()) {

                if (topologyCooldownBuffer.isCoolingDown(topologyID)) {
                    if(CONFIG_DEBUG)
                        log("[TOPOLOGY] topology " + topologyID + " is cooling down");
                    continue;
                }

                log((CONFIG_DEBUG ? "[TOPOLOGY]" : "" ) + " processing topology " + topologyID);

                // MONITOR
                // NOTE: sets migration notification on zookeeper for each exec.
                // As executors are being processed, their migration notification must be unset.
                List<AugExecutorContext> moveableExecutors = getMoveableExecutorsContext(topologies.getById(topologyID), topologyContexts.get(topologyID), cluster.getAssignmentById(topologyID));

                topology = topologies.getById(topologyID);
                assignment = assignments.get(topologyID);

                if(CONFIG_DEBUG)
                {
                    log("[MONITOR] [MOVEABLE] PRINTING MOVEABLE EXECUTORS in " + supervisor.getSupervisorId());
                    log("[MONITOR] [MOVEABLE] obtained " + moveableExecutors.size() + " executor(s)");
                    for(AugExecutorContext exCtx : moveableExecutors)
                    {
                        WorkerSlot localSlot =  exCtx.getAugExecutor().getWorkerSlot();
                        log("[MONITOR] [MOVEABLE] - " + (localSlot == null ? null : localSlot.getPort()) + " " + exCtx.getAugExecutor().getComponentId() + "(" + exCtx.getAugExecutor().getExecutor() + ")");
                        List<ExecutorDetails> source = new ArrayList<ExecutorDetails>();
                        List<ExecutorDetails> target = new ArrayList<ExecutorDetails>();
                        for(AugExecutorDetails aExec : exCtx.getNeighborExecutors())
                        {
                            if(exCtx.isTarget(aExec))   target.add(aExec.getExecutor());
                            else                        source.add(aExec.getExecutor());
                        }
                        log("[MONITOR] [MOVEABLE] - - Source: " + source);
                        log("[MONITOR] [MOVEABLE] - - Target: " + target);
                    }
                }

                for (AugExecutorContext executorContext : moveableExecutors) {

                    // if an executor has been previously moved, the topology is in cooldown state,
                    // thus must remove it from migrating and skip.
                    if(topologyCooldownBuffer.isCoolingDown(topologyID))
                    {
                        migrationTracker.unsetMigration(topologyID, executorContext.getAugExecutor().getComponentId());
                        continue;
                    }

                    List<NeighborNode> neighborNodes = computeNeighborNodes(executorContext, knownNodesCoordinates, topology);

                    if(CONFIG_DEBUG)
                    {
                        log("[MONITOR] [RESULT] processing " + executorContext.getAugExecutor().getComponentId() + " " + executorContext.getAugExecutor().getExecutor());
                        log("[MONITOR] [RESULT] " + neighborNodes.size() + " physical neighbor nodes:");
                        for(NeighborNode node : neighborNodes)
                        {
                            log("[MONITOR] [RESULT] - " + node.getNodeId());
                            log("[MONITOR] [RESULT] - + n. exec : " + node.getExecutors().size());
                            log("[MONITOR] [RESULT] - + distance: " + networkSpaceManager.getSpace().distance(node.getPosition(), localPosition));
                            log("[MONITOR] [RESULT] - + datarate: " + node.getDatarate());
                        }
                    }

                    // ANALYZE
                    Placement optimalPlacement = evaluateContinuousOptimalPlacement(neighborNodes);

                    if(CONFIG_DEBUG)
                        log("[ANALYZE] [RESULT] optimal placement: " + optimalPlacement);

                    if (optimalPlacement == null) {

                        migrationTracker.unsetMigration(topologyID, executorContext.getAugExecutor().getComponentId());
                        continue;
                    }


                    // PLAN
                    WorkerSlot chosenWorkerslot = findNearestWorkerSlot(
                            optimalPlacement,
                            neighborNodes,
                            knownNodesCoordinates,
                            topology,
                            cluster);

                    if(CONFIG_DEBUG)
                    {
                        if(chosenWorkerslot == null)
                            log("[PLAN] [RESULT] best placement is current node");
                        else {
                            log("[PLAN] [RESULT] target : " + (chosenWorkerslot != null ? chosenWorkerslot.getNodeId() : null));
                            log("[PLAN] [RESULT] slot   : " + (chosenWorkerslot != null ? chosenWorkerslot.getPort() : null));
                        }
                    }

                    if (chosenWorkerslot == null ||
                            chosenWorkerslot.getNodeId().equals(supervisor.getSupervisorId())) {
                        migrationTracker.unsetMigration(topologyID, executorContext.getAugExecutor().getComponentId());
                        continue;
                    }


                    // EXECUTE
                    boolean newAssignmentDone = commitNewExecutorAssignment(
                            chosenWorkerslot,
                            executorContext.getAugExecutor(),
                            topology,
                            cluster);

                    if(newAssignmentDone) {
                        AugExecutorDetails ex = executorContext.getAugExecutor();
                        log("Migration done (sys time: " + System.currentTimeMillis() + ")");
                        log("Migrated " + ex.getExecutor() + " " + ex.getComponentId());
                        log("From     " + ex.getWorkerSlot());
                        log("To       " + chosenWorkerslot);
                        topologyCooldownBuffer.set(topologyID);
                        // won't break here, because other moveable executors have been set to migrating
                        // (check comment at the for-cycle beginning)
                    }

                    // Unset migration notificatio on zookeeper
                    migrationTracker.unsetMigration(topologyID, executorContext.getAugExecutor().getComponentId());
                }
            }
            long end = System.currentTimeMillis();

            if(CONFIG_DEBUG) {
                long elapsed = end - start;
                log("[TIME] end of scheduling (" + end + ")");
                log("[TIME] time elapsed: " + elapsed / 1000 + " seconds and " + elapsed % 1000 + " milliseconds.");
            }
            else
            {
                log("SCHEDULING FINISHED (" + end + ")");
            }
        }
        catch(Exception ex)
        {
            System.out.println("Exception " + ex.getClass().getSimpleName() + " : " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * Finds moveable executors and return them as AugExecutorContext object.
     *
     * An executor is moveable if:
     * - It's not a system component (i.e. does not start with "__")
     * - Within the topology graph, has at least 1 application component as parent and 1 as child
     * - Within the cluster, no other executor related to parent/child component is being migrated
     * - Within the cluster, no other executor related to same component is being migrated, unless it's local executor
     *
     * If the executor meets the previous criteria, it's created the related AugExecutorContext, which contains:
     * - executor relevant info (-> AugExecutorDetails)
     * - a list of executor related to parent/child components (->List<AugExecutorDetails>)
     * - an index on which executor in list is parent (= !target) or child (= target)
     *
     * @param topology: topology on which the search will be done
     * @param context: topology context
     * @param assignment: scheduling assignment information
     * @return: a list of AugExecutorContext objects
     */
    private List<AugExecutorContext> getMoveableExecutorsContext(TopologyDetails topology, GeneralTopologyContext context , SchedulerAssignment assignment)
    {
        List<AugExecutorContext> moveableExecutors = new ArrayList<AugExecutorContext>();

        // preparing stuff
        if(assignment == null) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> executorToWorkerSlot = assignment.getExecutorToSlot();
        if(executorToWorkerSlot == null || executorToWorkerSlot.isEmpty()) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> localExecutorToWorkerSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        String localNodeID = supervisor.getSupervisorId();

        // still preparing..
        for(ExecutorDetails exec : executorToWorkerSlot.keySet())
        {
            WorkerSlot wSlot = executorToWorkerSlot.get(exec);

            if(wSlot != null && localNodeID.equals(wSlot.getNodeId()))
                localExecutorToWorkerSlot.put(exec, wSlot);
        }

        boolean aRelatedComponentIsMigrating;
        boolean currentComponentIsLeafNode;
        boolean currentComponentIsRootNode;

        List<String> sourceComponents;
        List<String> targetComponents;

        for(ExecutorDetails localExecutor : localExecutorToWorkerSlot.keySet())
        {
			/*	localExecutor cannot be moved if its corresponding localComponent:
			 *  -	is System Component (null id or starts with "__")
			 *  - 	is migrating already
			 *  -   has source or target component which is migrating
			 *  -	is a leaf node (all target are System Component)
			 *  -	is a root node (all source are System Component)
			 */


            String componentID = topology.getExecutorToComponent().get(localExecutor);

            // avoid null components and pinned components
            if(componentID == null || componentID.startsWith(SYSTEM_COMPONENT_PREFIX))
                continue;

            // avoid migrating components
            if (migrationTracker.isMigrating(topology.getId(), componentID))
                continue;

            // These are "assumptions", updated during execution
            aRelatedComponentIsMigrating = false;
            currentComponentIsLeafNode = true;
            currentComponentIsRootNode = true;

            // avoid component whose target component is migrating
            targetComponents = getTargetComponentIds(componentID, context);

            for(String targetComponentID : targetComponents)
            {

                if(targetComponentID == null)
                {
                    // TODO: is this the correct way to deal with null string components? Does it even have any sense?
                    continue;
                }

                if(!targetComponentID.startsWith(SYSTEM_COMPONENT_PREFIX))
                {
                    //System.out.println(" - Target Component " + targetComponentID + " is System Component -> component " + componentID + " is NOT moveable.");
                    //LOG.info(" - Target Component {} is System Component -> component {} is NOT moveable.", targetComponentID, componentID);
                    //System.out.println(LOG_TAG + " - target is System Component -> component is Not moveable ");
                    currentComponentIsLeafNode=false;
                }

                if (migrationTracker.isMigrating(topology.getId(), targetComponentID))
                {
                    //System.out.println(" - Target Component " + targetComponentID + " is Migrating -> component " + componentID + " is NOT moveable.");
                    //LOG.info(" - Target Component {} is Migrating -> component {} is NOT moveable.", targetComponentID, componentID);
                    aRelatedComponentIsMigrating=true;
                    //System.out.println(LOG_TAG + " - target is migrating -> component is Not moveable ");
                    break;
                }
            }

            if(aRelatedComponentIsMigrating || currentComponentIsLeafNode)
                continue;

            // avoid components whose source node is migrating or

            sourceComponents = getSourceComponentIds(componentID, context);

            for(String sourceComponentID : sourceComponents)
            {
                //System.out.println(LOG_TAG + " - source: " + sourceComponentID);
                if(sourceComponentID == null)
                {
                    // TODO: is this the correct way to deal with null string components? Does it even have any sense?
                    continue;
                }

                if(!sourceComponentID.startsWith(SYSTEM_COMPONENT_PREFIX))
                {
                    //System.out.println(" - Source Component " + sourceComponentID + " is System Component -> component " + componentID + " is NOT moveable.");
                    currentComponentIsRootNode = false;
                 }

                if (migrationTracker.isMigrating(topology.getId(), sourceComponentID))
                {
                    //System.out.println(" - Source Component " + sourceComponentID + " is Migrating -> component " + componentID + " is NOT moveable.");
                    aRelatedComponentIsMigrating=true;
                    break;
                }
            }

            if(aRelatedComponentIsMigrating || currentComponentIsRootNode)
                continue;

            // re-checks if component is migrating, then set migrating
            if (migrationTracker.isMigrating(topology.getId(), componentID))
                continue;

            migrationTracker.setMigration(topology.getId(), componentID);

            AugExecutorContext augExecContext = new AugExecutorContext(new AugExecutorDetails(localExecutor, topology, assignment));
            augExecContext.addNeighbors(topology, context, assignment);
            moveableExecutors.add(augExecContext);
        }
        return moveableExecutors;
    }

    /**
     * Within a topology graph, finds the reference component's parents
     *
     * @param referenceComponentId: id of the component whose parents are to be found
     * @param context: component related topology context
     * @return: list of component ids, parents of reference component
     */
    private List<String> getTargetComponentIds(String referenceComponentId, GeneralTopologyContext context)
    {
        List<String> targetComponents = new ArrayList<String>();

        Map<String, Map<String, Grouping>> targets = context.getTargets(referenceComponentId);

        for(String streamId : targets.keySet()){
            Set<String> componentsId = targets.get(streamId).keySet();

            if (streamId!=null && streamId.equals("default"))
                targetComponents.addAll(componentsId);
        }
        return targetComponents;
    }

    /**
     * Within a topology graph, finds the reference component's childs
     *
     * @param referenceComponentId: id of the component whose childs are to be found
     * @param context: component related topology context
     * @return: list of component ids, childs of reference component
     */

    private List<String> getSourceComponentIds(String referenceComponentId, GeneralTopologyContext context)
    {
        List<String> sourceComponents = new ArrayList<String>();

        Map<GlobalStreamId, Grouping> sources = context.getSources(referenceComponentId);
        for(GlobalStreamId globalStreamID : sources.keySet()){
            if (globalStreamID!=null && !globalStreamID.get_componentId().startsWith(SYSTEM_COMPONENT_PREFIX))
                sourceComponents.add(globalStreamID.get_componentId());
        }

        return sourceComponents;
    }

    /**
     * Generates the set of neighbor nodes, each containing Point position on the used continuous space
     * and the Double datarate from/to it.
     *
     * @param executorContext       : executor with context information
     * @param knownNodesCoordinates : known nodes coordinates (if some missing, asks zookeeper)
     * @param topology              : topology details
     * @return                      : Set of Neighbor Nodes, containing node info and data
     */

    private List<NeighborNode> computeNeighborNodes(AugExecutorContext executorContext, Map<String, Node> knownNodesCoordinates, TopologyDetails topology)
    {
        List<NeighborNode> nodes = new ArrayList<NeighborNode>();

        String nodeId;
        Map<String, List<AugExecutorDetails>> node2nEx = new HashMap<String, List<AugExecutorDetails>>();
        List<AugExecutorDetails> neighborExecutors = executorContext.getNeighborExecutors();

        for(AugExecutorDetails neighborExec : neighborExecutors)
        {
            nodeId = neighborExec.getNodeId();
            if(!node2nEx.containsKey(nodeId))
                node2nEx.put(nodeId, new ArrayList<AugExecutorDetails>());

            node2nEx.get(nodeId).add(neighborExec);
        }

        NeighborNode node;
        Point position;
        Double datarate;

        for(String neighborNodeId : node2nEx.keySet())
        {
            if(knownNodesCoordinates.containsKey(neighborNodeId))
                position = knownNodesCoordinates.get(neighborNodeId).getCoordinates();
            else {
                Node n = networkSpaceManager.retrieveCoordinatesFromZK(neighborNodeId);
                knownNodesCoordinates.put(neighborNodeId, n);
                position = n.getCoordinates();
            }
            datarate = computeNeighborNodeDatarate(executorContext, node2nEx.get(neighborNodeId), topology);

            node = new NeighborNode(node2nEx.get(neighborNodeId), position, datarate);

            nodes.add(node);
        }
        return nodes;
    }

    /**
     * Evaluates the optimal placement, based on the neighbor nodes.
     *
     * @param neighborNodes     : set of neighbor nodes (only Point position and Double datarate is used)
     * @return                  : optimal placement on continuous space
     */

    private Placement evaluateContinuousOptimalPlacement(
            List<NeighborNode> neighborNodes)
    {

        Placement optimalPlacement = computeWeightedMedianPointPlacement(neighborNodes);

        if(CONFIG_DEBUG)
            log("[ANALYZE] [RESULT] [PARTIAL] initial placement " + optimalPlacement);

        optimalPlacement = checkDeadPointsForBetterPlacement(optimalPlacement, neighborNodes);

        if(CONFIG_DEBUG)
            log("[ANALYZE] [RESULT] [PARTIAL] after neighbor check " + optimalPlacement);

        optimalPlacement = computeOptimalPlacementByGradient(optimalPlacement, neighborNodes);

        if(CONFIG_DEBUG)
            log("[ANALYZE] [RESULT] final placement " + optimalPlacement);

        return optimalPlacement;
    }

    /**
     * Computes the best placement over a continuous space (latency space). Starting from an initial placement,
     * iteratively evaluates the gradient vector the following way:
     *
     * - direction is based on the weighted sum of distances from neighbors (weights being the datarates to nodes)
     * - modulus (called gradientStep) is set as the farthest distance from initial placement and other neighbor nodes.
     *
     * At each iteration, the position is moved according to the gradient vector. If new position has lower cost, then
     * the position is moved and gradient reevaluated, otherwise the gradientStep is halved and the positioning repeated.
     *
     * @param initialPlacement  : initial placement (Point position + Double related network cost)
     * @param nodes             : neighbor nodes, containing position and datarates to nodes
     * @return                  : optimal placement on the continuous space
     */

    private Placement computeOptimalPlacementByGradient(Placement initialPlacement, List<NeighborNode> nodes)
    {
        // TODO: as for now, point sum/mult is done NOT using the Space class provided by QOSMonitor.
        // should add a method for weighted sum --> sum(pA, pB, weight)

        if(CONFIG_DEBUG)
            log("[ANALYZE] [GRADIENT] applying gradient method");

        int dimensionality = networkSpaceManager.getSpace().getLatencyDimensions();
        Placement currentOptimalPlacement = new Placement(initialPlacement);
        Point gradientVector = new Point(dimensionality),
                currentPosition = currentOptimalPlacement.getPosition().clone();
        Double gradientStep = computeFarthestPointDistance(initialPlacement.getPosition(), nodes, dimensionality);
        Double segment, mainFlowModulus, gradientModulusCorrection;

        Double improvement, currentCost = Double.MAX_VALUE;
        Integer retries;
        retries = CONFIG_ANALYZE_MAX_RETRIES;

        do
        {
            // reset improvement
            improvement = -1.0;

            // reset gradient vector
            for(int dim=0; dim<dimensionality; dim++)
                gradientVector.set(dim, 0);

            mainFlowModulus = 0.0;

            // evaluates the main flow: each link to a neighbor is considered as an outgoing vector
            // with modulus=latency and weight=datarate
            for(NeighborNode node : nodes) {

                for (int dim = 0; dim < dimensionality; dim++) {
                    segment = (node.getPosition().get(dim) - currentPosition.get(dim)) * node.getDatarate();
                    gradientVector.set(
                            dim,
                            gradientVector.get(dim) + segment
                    );
                    mainFlowModulus += Math.pow(segment, 2);
                }
            }

            if(CONFIG_DEBUG)
                log("[ANALYZE] [GRADIENT] main flow: " + gradientVector);

            // if gradient/mainFlow modulus is 0, minimum is reached --> job is done!
            if(mainFlowModulus == 0) {
                if(CONFIG_DEBUG)
                    log("[ANALYZE] [GRADIENT] gradient modulus: 0 (minimum reached!)");
                break;
            }

            mainFlowModulus = Math.sqrt(mainFlowModulus);

            // gradientModulusCorrection is used to shrink/nelarge gradient modulus
            // so that the modulus is equal to the required gradientStep
            gradientModulusCorrection = gradientStep/mainFlowModulus;

            if(CONFIG_DEBUG)
            {
                log("[ANALYZE] [GRADIENT] gradient modulus: " + mainFlowModulus);
            }

            do{
                // step 1: set correct gradient vector (i.e. adjust modulus)
                // step 2: move current position according to modulus
                // --> it's done on each dimension
                for(int dim=0; dim<dimensionality; dim++)
                {
                    // step 1
                    gradientVector.set(dim, gradientVector.get(dim) * gradientModulusCorrection);
                    // step 2
                    currentPosition.set(dim, currentOptimalPlacement.getPosition().get(dim) + gradientVector.get(dim));
                }

                currentCost = evaluatePositioningCost(currentPosition, nodes);

                if(currentCost<currentOptimalPlacement.getCost())
                {
                    improvement =  currentCost - currentOptimalPlacement.getCost() ;
                    currentOptimalPlacement.setPosition(currentPosition.clone());
                    currentOptimalPlacement.setCost(currentCost);

                    if(CONFIG_DEBUG)
                    {
                        log("[ANALYZE] [GRADIENT] new placement: " + currentOptimalPlacement);
                    }

                    break;
                }
                else
                {

                    gradientStep = gradientStep/2;
                    gradientModulusCorrection = 0.5; // ratio set to 0.5 == next gradient modulus is halved
                    retries--;

                }

                // TODO: the criterion for stopping is correct? can be improved?
            }while((retries) >= 0);

            if(retries < 0 && CONFIG_DEBUG)
                    log("[ANALYZE] [GRADIENT] no more retries.");

        } while(improvement > CONFIG_ANALYZE_IMPROVEMENT_THRESHOLD || retries >= 0);


        return currentOptimalPlacement;
    }

    /**
     * Evaluate the farthest distance between reference point and neighboring nodes
     *
     * @param referencePoint    : reference point
     * @param nodes             : neighboring nodes
     * @param dimensionality    : max dimensionality to consider on point
     * @return                  : farthest distance
     */

    private Double computeFarthestPointDistance(Point referencePoint, List<NeighborNode> nodes, int dimensionality)
    {
        Double squareSum, distance;
        Double currentFarthestDistance = 0.0;
        for(NeighborNode node : nodes)
        {
            squareSum = 0.0;

            for(int dim = 0; dim < dimensionality; dim ++)
            {
                squareSum += Math.pow( (referencePoint.get(dim) - node.getPosition().get(dim)) , 2 );
            }

            distance = Math.sqrt(squareSum);

            if(distance > currentFarthestDistance)
                currentFarthestDistance = distance;
        }

        return currentFarthestDistance;
    }

    /**
     * Taking an initial placement (Point position + Double cost) as a reference, and a set of
     * neighboring nodes, evaluates the network costs originated by each positioning on an already
     * existent neighbor node and confront that cost with the initial one. Returns the better placement
     * among the initial positioning and the neighbors ones.
     *
     * @param initialPlacement  : reference Placement object
     * @param neighborNodes     : set of neighbor node
     * @return                  : Placement object, which is the best placement among initial one and those evaluated on neighbor nodes
     */

    private Placement checkDeadPointsForBetterPlacement(
            Placement initialPlacement,
            List<NeighborNode> neighborNodes
    )
    {
        Placement currentOptimalPlacement = initialPlacement;
        Double cost;

        for(NeighborNode neighbor : neighborNodes)
        {
            cost = evaluatePositioningCost(neighbor.getPosition(), neighborNodes);
            if(cost<currentOptimalPlacement.getCost())
            {
                currentOptimalPlacement = new Placement(neighbor.getPosition(), cost);
            }
        }
        return currentOptimalPlacement;
    }

    /**
     * Computes the placement based on weighted median point among a set of nodes (position + datarate).
     * Weight used is the datarate of each node in the set.
     *
     * @param nodes             : nodes on which compute the weighted median point
     * @return                  : median placement (contains position an placement cost)
     */

    private Placement computeWeightedMedianPointPlacement(List<NeighborNode> nodes)
    {
        int dimensionality = networkSpaceManager.getSpace().getLatencyDimensions();
        Point position = new Point(dimensionality),
                neighborPosition;

        Double weightedSum = 0.0, sumOfWeights = 0.0;

        for(NeighborNode node : nodes)
        {
            sumOfWeights += node.getDatarate();
            neighborPosition = node.getPosition();
            for(int dim=0; dim<dimensionality; dim++)
            {
                position.set(dim, position.get(dim) + neighborPosition.get(dim) * node.getDatarate());
            }

        }

        if(sumOfWeights != 0.0)
            for(int dim=0; dim<dimensionality; dim++)
            {
                position.set(dim, position.get(dim) / sumOfWeights);
            }
        else
        {
            // no neighbors/datarates = no traffic ... better stay here!
            position = networkSpaceManager.getCoordinates();
        }

        return new Placement(position, evaluatePositioningCost(position, nodes));
    }

    /**
     * Evaluates the cost related to the specified position. Given a position (Point) and a set of
     * neighboring nodes (Point + Datarate), evaluates the cost as:
     *
     *      SUM[node]{ (node.position - specified_position) * node.datarate }
     *
     * @param position  : Point object on which evaluate the network cost
     * @param nodes     : Set of neighboring nodes, of which we use the position (Point) and datarate (Double)
     * @return          : The resulting network cost
     */

    private Double evaluatePositioningCost(Point position, List<NeighborNode> nodes)
    {
        Double cost = 0.0, squareDistance;
        for(NeighborNode node : nodes)
        {
            squareDistance = 0.0;
            for(int dim=0; dim < position.getDimensionality(); dim ++)
            {
                squareDistance += Math.pow( (node.getPosition().get(dim) - position.get(dim)), 2 );
            }
            cost += Math.sqrt(squareDistance)*node.getDatarate();
        }
        return cost;
    }


    private Double computeNeighborNodeDatarate(AugExecutorContext localExecutorContext, List<AugExecutorDetails> neighborExecutors, TopologyDetails topology)
    {
        Double datarate = 0.0;
        ExecutorDetails localExecutor = localExecutorContext.getAugExecutor().getExecutor();
        ExecutorDetails otherExecutor = null;
        for(AugExecutorDetails neighborExecutor : neighborExecutors)
        {
            otherExecutor = neighborExecutor.getExecutor();

            if(localExecutorContext.isTarget(neighborExecutor))
                datarate += computeExecutorsDatarate(localExecutor, otherExecutor, topology);
            else
                datarate += computeExecutorsDatarate(otherExecutor, localExecutor, topology);
        }
        return datarate;
    }

    private Double computeExecutorsDatarate(ExecutorDetails sourceExecutor, ExecutorDetails targetExecutor, TopologyDetails topology)
    {
        Double datarate = 0.0;
        List<Measurement> measurements;

        for(int sourceTask = sourceExecutor.getStartTask() ; sourceTask < (sourceExecutor.getEndTask() + 1); sourceTask++)
        {
            for(int targetTask = targetExecutor.getStartTask() ; targetTask < (targetExecutor.getEndTask() + 1); targetTask++)
            {
                try {
                    measurements = databaseManager.getMeasurements(Integer.toString(sourceTask), Integer.toString(targetTask), topology.getId());

                    if (measurements != null && !measurements.isEmpty())
                        datarate += measurements.get(0).getValue();
                }
                catch (Exception ex)
                {
                    // TODO: better handling of the exception
                    ex.printStackTrace();
                }
            }
        }

        return datarate;
    }

    /**
     *  Commits the planned executor migration.
     *
     * @param targetWorkerSlot : worker slot on which the executor will be places
     * @param executorToMigrate : details about the executor to migrate
     * @param topology : topology related to the migrating executor
     * @param cluster
     * @return
     */
    private boolean commitNewExecutorAssignment(
            WorkerSlot targetWorkerSlot,
            AugExecutorDetails executorToMigrate,
            TopologyDetails topology,
            Cluster cluster)
    {
        if(targetWorkerSlot == null)
        {
            if(CONFIG_DEBUG)
                log("[EXECUTE] [RESULT] null target, abort");
            migrationTracker.unsetMigration(topology.getId(), executorToMigrate.getComponentId());
            return false;
        }


        if(CONFIG_DEBUG)
            log("[EXECUTE] [RESULT] migrating " + executorToMigrate.getExecutor() + " " + executorToMigrate.getComponentId());

        // Consider the executor to move, then its original (source) worker slot
        // and its destination (target) worker slot.
        //
        // In order to reassign the executor, both source and target worker slot
        // must be freed. This way, also other executor (in both source and target)
        // should be reassigned (to their original worker slots).

        SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());
        ExecutorDetails executorToMove = executorToMigrate.getExecutor();
        Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();
        WorkerSlot sourceWorkerSlot = ex2ws.get(executorToMove);

        List<ExecutorDetails> executorToAssignOnSourceWS = new ArrayList<ExecutorDetails>(),
                executorToAssignOnTargetWS = new ArrayList<ExecutorDetails>();

        for(ExecutorDetails executor : ex2ws.keySet())
        {
            // executor to move must be assigned to target worker slot
            if(executor.equals(executorToMove)) {
                executorToAssignOnTargetWS.add(executor);
                continue;
            }

            WorkerSlot workerSlot = ex2ws.get(executor);

            // other executors must remain on their original slot
            if(workerSlot.equals(sourceWorkerSlot))
            {
                // once source worker slot is freed, executor must be reassigned
                executorToAssignOnSourceWS.add(executor);
            }
            else if(workerSlot.equals(targetWorkerSlot))
            {
                // once target worker slot is freed, executor must be reassigned
                executorToAssignOnTargetWS.add(executor);
            }
        }


        if(CONFIG_DEBUG) {
            log("[EXECUTE] executors to set on TARGET " + targetWorkerSlot);
            log("[EXECUTE] -  " + executorToAssignOnTargetWS);
            log("[EXECUTE] executors to set on LOCAL  " + sourceWorkerSlot);
            log("[EXECUTE] -  " + executorToAssignOnSourceWS);
            log("[EXECUTE] committing changes ...");
        }
        try {
            // reassign executors to source worker slot
            cluster.freeSlot(sourceWorkerSlot);
            if (!executorToAssignOnSourceWS.isEmpty())
                cluster.assign(sourceWorkerSlot, topology.getId(), executorToAssignOnSourceWS);

            // reassign executors to target worker slot
            cluster.freeSlot(targetWorkerSlot);
            if (!executorToAssignOnTargetWS.isEmpty())
                cluster.assign(targetWorkerSlot, topology.getId(), executorToAssignOnTargetWS);
        }
        catch (RuntimeException ex)
        {
            log("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }
        catch (Exception ex)
        {
            log("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }

        // migration is "done" (differred, but ready), unset migration status
        migrationTracker.unsetMigration(topology.getId(), executorToMigrate.getComponentId());

        // setting cooldown for topologyId
        topologyCooldownBuffer.set(topology.getId());

        // since executor is being moved, its related measurements must be cancelled from db
        try {
            for (int task = executorToMove.getStartTask(); task < (executorToMove.getEndTask() + 1); task++)
                databaseManager.deleteMeasurements(Integer.toString(task), topology.getId());
        }
        catch(Exception ex)
        {
            // TODO: better exception handling
            ex.printStackTrace();
        }

        if(CONFIG_DEBUG)
            log("[EXECUTE] [RESULT] done.");

        return true;
    }


    private WorkerSlot findNearestWorkerSlot(
            Placement chosenPlacement,
            List<NeighborNode> nodes,
            Map<String, Node> knownNodes,
            TopologyDetails topology,
            Cluster cluster
    )
    {
        if (networkSpaceManager == null)
            return null;

        Placement initialPlacement = new Placement(networkSpaceManager.getCoordinates());
        initialPlacement.setCost(evaluatePositioningCost(initialPlacement.getPosition(), nodes));

        Placement candidatePlacement = null;
        KNearestNodes knn = new SimpleKNearestNodes(networkSpaceManager.getSpace());

        List<KNNItem> nearestNodes = knn.getKNearestNode(10, chosenPlacement.getPosition(), knownNodes);
        Node actualNode;
        String actualNodeSupervisorID;

        if(CONFIG_DEBUG)
        {
            log("[PLAN] [NEAREST] SEARCHING NODE nearest to " + chosenPlacement);
            log("[PLAN] [NEAREST] (current placement: " + initialPlacement + ")");
            log("[PLAN] [NEAREST] checking nodes among " + nearestNodes.size() + " nearest");
        }

        for(KNNItem item : nearestNodes)
        {
            // if stuff is null, continue with another item
            if((actualNode = item.getNode()) == null || (actualNodeSupervisorID = actualNode.getSupervisorId()) == null)
                continue;

            if(CONFIG_DEBUG) {

                log("[PLAN] [NEAREST] node: " + actualNode.getSupervisorId());
            }


            // check if candidate node is current node, if yes no migration necessary (null)
            if(actualNodeSupervisorID.equals(supervisor.getSupervisorId())) {
                if(CONFIG_DEBUG) {
                    log("[PLAN] [NEAREST] -> node is current!");
                }

                // TODO: should continue, since nodes which are (slightly?) farther from ideal placement than local node could have better cost.. but then, when to stop? should scan all the K nearest?
                break;
            }
            // check if migration is convenient, if not then continue with another item
            candidatePlacement = new Placement(actualNode.getCoordinates());
            candidatePlacement.setCost(evaluatePositioningCost(candidatePlacement.getPosition(), nodes));

            if(CONFIG_DEBUG)
            {
                log("[PLAN] [NEAREST] - + cost:     " + candidatePlacement.getCost());
            }

            if(candidatePlacement.getCost() > initialPlacement.getCost())
            {
                if(CONFIG_DEBUG) {
                    log("[PLAN] [NEAREST] -> higher cost than current node!");
                }
                // TODO: should continue, since nodes which are (slightly?) farther from ideal placement than current candidate node could have better cost.. but then, when to stop? should scan all the K nearest?
                break;
            }

            // check if migration improvement is acceptable, if not then continue with another item
            Double migrationImprovement = initialPlacement.getCost() - candidatePlacement.getCost();
            Double migrationImprovementTreshold = CONFIG_PLAN_MIGRATION_THRESHOLD;

            if(migrationImprovement < migrationImprovementTreshold)
            {
                if(CONFIG_DEBUG)
                {
                    log("[PLAN] [NEAREST] -> improvement is not enough! (" + migrationImprovement + ")");
                    log("[PLAN] [NEAREST] -> required: " + migrationImprovementTreshold);
                }
                continue;
            }

            Double relativeCostTreshold = (initialPlacement.getCost() - chosenPlacement.getCost()) * (1-CONFIG_PLAN_MIGRATION_IMPROVEMENT_FRACTION);
            Double actualRelativeCost = (candidatePlacement.getCost() - chosenPlacement.getCost());

            if(actualRelativeCost > relativeCostTreshold)
            {
                if(CONFIG_DEBUG)
                {
                    log("[PLAN] [NEAREST] -> relative cost is too high! (" + actualRelativeCost + ")");
                    log("[PLAN] [NEAREST] -> treshold: " + relativeCostTreshold);
                }
                continue;
            }

            // finds an available slot on target node, if any available
            WorkerSlot availableSlot = findAvailableWorkerSlotOnTargetNode(item.getNode(), topology, cluster);



            // if none is available, continue with another item
            if(availableSlot == null)
            {
                if(CONFIG_DEBUG) {
                    log("[PLAN] [NEAREST] -> no available slots!" );
                }
                continue;
            }

            if(CONFIG_DEBUG) {
                log("[PLAN] [RESULT] slot found: " + availableSlot);
            }

            // slot found, returning slot
            return availableSlot;
        }

        if(CONFIG_DEBUG)
            log("[PLAN] [RESULT] could not find a good node.");

        // scanned all items, migration cannot be done
        return null;
    }

    private WorkerSlot findAvailableWorkerSlotOnTargetNode(Node workerNode, TopologyDetails topology, Cluster cluster)
    {

        // first, try to get a slot which is processing the same topology

        SupervisorDetails supervisor = cluster.getSupervisorById(workerNode.getSupervisorId());
        SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());

        Map<WorkerSlot, Integer> executorCounter = new HashMap<WorkerSlot, Integer>();
        Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();

        for(ExecutorDetails exec : ex2ws.keySet())
        {
            WorkerSlot ws = ex2ws.get(exec);

            if(!ws.getNodeId().equals(workerNode.getSupervisorId()))
                continue;

            if(!executorCounter.containsKey(ws))
                executorCounter.put(ws, 1);
            else
                executorCounter.put(ws, 1 + executorCounter.get(ws));
        }

        WorkerSlot lessCrowdedWorkerSlot = null;
        Integer minimumExecutorCount = Integer.MAX_VALUE;


        if(CONFIG_DEBUG)
        {
            log("[PLAN] [SLOTS] searching slot on " + workerNode.getSupervisorId());
            log("[PLAN] [SLOTS] -> searching reusable slot, among " + executorCounter.keySet().size() + ".");
        }
        // choose the one with less executors
        for(WorkerSlot ws : executorCounter.keySet())
        {
            log("[PLAN] [SLOTS] -> slot " + ws.getPort() + " has " + executorCounter.get(ws) + " executors.");
            if(executorCounter.get(ws) < minimumExecutorCount)
            {
                minimumExecutorCount = executorCounter.get(ws);
                lessCrowdedWorkerSlot = ws;
            }
        }

        // if no worker slot is found from previous steps or the less crowded is still too crowded,
        // get the first one available

        if(lessCrowdedWorkerSlot != null && executorCounter.get(lessCrowdedWorkerSlot) < CONFIG_PLAN_MAX_EXEC_PER_SLOT)
        {
            if(CONFIG_DEBUG) {
                log("[PLAN] [SLOTS] -> found reusable slot: " + lessCrowdedWorkerSlot);
            }
            return lessCrowdedWorkerSlot;
        }

        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);

        if(CONFIG_DEBUG) {
            log("[PLAN] [SLOTS] -> could not find a reusable slot");

            if(availableSlots == null || availableSlots.isEmpty())
                log("[PLAN] [SLOTS] -> could not find an available slot neither");
            else
                log("[PLAN] [SLOTS] -> found available slot: " + availableSlots.get(0).getPort());
        }

        return (availableSlots == null || availableSlots.isEmpty() ? null : availableSlots.get(0));
    }

}
