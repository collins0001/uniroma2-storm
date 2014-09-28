package it.uniroma2.adaptivescheduler.scheduler;

import it.uniroma2.adaptivescheduler.common.Point;
import it.uniroma2.adaptivescheduler.common.Serializer;
import it.uniroma2.adaptivescheduler.common.Space;
import it.uniroma2.adaptivescheduler.common.SpaceFactory;
import it.uniroma2.adaptivescheduler.networkspace.KNNItem;
import it.uniroma2.adaptivescheduler.networkspace.KNearestNodes;
import it.uniroma2.adaptivescheduler.networkspace.Node;
import it.uniroma2.adaptivescheduler.networkspace.SimpleKNearestNodes;
import it.uniroma2.adaptivescheduler.scheduler.internal.ExecutorPool;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.GeneralTopologyContext;

public class InitialScheduler implements IScheduler{

	/* Const */
	private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";
	private static final String ZK_COORDINATES_DIR = "/extension/networkspace/coordinates";
	private static final String NIMBUS_STUB_NODE_ID = "nimbus";

	private double SPRING_FORCE_THRESHOLD = 1.0;
	private double SPRING_FORCE_DELTA = 0.1;
	private int K_NEAREST_NODE_TO_RETRIEVE = 7;
	
	
	private static final boolean DEBUG_RELAXATION_PLACEMENT = false;
	
	private SimpleZookeeperClient zkClient = null; 

	@SuppressWarnings("rawtypes")
	private Map config;

	private boolean locationAwareScheduling = true;

	private Space networkSpace;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		
		this.config = conf;
		
		readConfig(conf);

		this.networkSpace = SpaceFactory.createSpace();
		
		
		/* Create managers */
		createZookeeperClient();
		
	}
	
	@SuppressWarnings("rawtypes") 
	private void readConfig(Map config){
		if (config != null){
			Boolean bValue = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_INITIAL_SCHEDULER_LOCATION_AWARE);
			if (bValue != null){
				locationAwareScheduling = bValue.booleanValue();
				System.out.println("Location-aware scheduling: " + locationAwareScheduling);
			}
			
			Double dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_FORCE_THRESHOLD);
			if (dValue != null){
				SPRING_FORCE_THRESHOLD = dValue.doubleValue();
				System.out.println("Read spring force threshold: " + SPRING_FORCE_THRESHOLD);
			}
			
			dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_FORCE_DELTA);
			if (dValue != null){
				SPRING_FORCE_DELTA = dValue.doubleValue();
				System.out.println("Read delta: " + SPRING_FORCE_DELTA);
			}
			
			Boolean extendedSpace = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_USE_EXTENDED_SPACE);
			if (extendedSpace != null && extendedSpace.booleanValue() == true){
				SpaceFactory.setUseExtendedSpace(true);
				System.out.println("Using extended space");
			}

		}
	}
	
	@SuppressWarnings("unchecked")
	private void createZookeeperClient(){
		
		/* Set default value */
		zkClient = null;

		/* ZKClient can be created only if configuration file has been read correctly */
		if (config != null){
			try {
				/* Retrieve ZK connection parameters */
				Integer port = (Integer) config.get(Config.STORM_ZOOKEEPER_PORT);
				Object obj = config.get(Config.STORM_ZOOKEEPER_SERVERS);
				if (obj instanceof List){
					List<String> servers = (List<String>) obj;
				
					/* Create ZK client
					 * NOTE: connection is done asynchronously */
					zkClient = new SimpleZookeeperClient(servers, port);

					// DEBUG: 
					System.out.println("Connecting to ZooKeeper");
					/* Initialization need to write to zookeeper. Wait until a connection is established */
					while(!zkClient.isConnected()){
						try {
							// DEBUG: 
							System.out.print(".");
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					// DEBUG: 
					System.out.println();
					System.out.println("ZkClient Created!");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	



	@Override
	public void schedule(Topologies topologies, Cluster cluster) {

		System.out.println("Executing INITIAL SCHEDULER ");
		
		List<TopologyDetails> topologiesToSchedule = cluster.needsSchedulingTopologies(topologies);
		
		/* Get the list of available slots */
		List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
		availableSlots = orderAvailableSlots(availableSlots);
		
		if (topologiesToSchedule != null){
			
			for(TopologyDetails topology : topologiesToSchedule){
				scheduleTopologyUsingRoundRobin(topology, availableSlots, cluster);
			}
			
		} else {
			System.out.println(" > No scheduling actions needed");
		}
    }
	


	public void scheduleUsingContext(Topologies topologies, Cluster cluster, Map<String, GeneralTopologyContext> topologiesContext) {

		System.out.println("Executing INITIAL SCHEDULER using Context");
		
		List<TopologyDetails> topologiesToSchedule = cluster.needsSchedulingTopologies(topologies);
		
		/* Get the list of available slots */
		List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
		availableSlots = orderAvailableSlots(availableSlots);
		
		
		if (topologiesToSchedule != null){
			
			for(TopologyDetails topology : topologiesToSchedule){
				String topologyId = topology.getId();
				GeneralTopologyContext topologyContext = topologiesContext.get(topologyId);
				Map<String, Node> knownNetworkSpaceNodes = new HashMap<String, Node>();
				if (cluster.needsScheduling(topology)){
					System.out.println("Topology " + topologyId + " needs scheduling ");
					retrieveAllNetworkSpaceNodes(cluster, knownNetworkSpaceNodes);
					scheduleTopology(topology, topologyContext, availableSlots, cluster, knownNetworkSpaceNodes);
				} else {
					System.out.println("Topology " + topologyId + " doesn't need scheduling ");
				}
			}
			
		} else {
			System.out.println(" > No scheduling actions needed");
		}
    }
	
	private void retrieveAllNetworkSpaceNodes(Cluster cluster, Map<String, Node> networkSpaceNodes){
		
		Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
		
		Set<String> supervisorIds = supervisors.keySet();
		
		for (String supervisorId : supervisorIds){

			Node coordinates = retrieveCoordinatesFromZK(supervisorId);
			if (coordinates != null)
				networkSpaceNodes.put(supervisorId, coordinates);
		}
		
	}
	
	/**
	 * Order WorkerSlot according to its port. Returned list can be used to spread
	 * executors in a round robin manner.
	 * @param workerSlots
	 * @return
	 */
	private List<WorkerSlot> orderAvailableSlots (List<WorkerSlot> workerSlots){
		
		Map<Integer, List<WorkerSlot>> map = new HashMap<Integer, List<WorkerSlot>>();
		
		for(WorkerSlot ws : workerSlots){
			
			Integer port = new Integer(ws.getPort());
			
			List<WorkerSlot> slots = map.get(port);
			
			if (slots == null)
				slots = new ArrayList<WorkerSlot>();
			
			slots.add(ws);
			map.put(port, slots);
			
		}

		List <WorkerSlot> orderedList = new ArrayList<WorkerSlot>();

		for (Integer port : map.keySet()){
			
			List<WorkerSlot> wss = map.get(port);
			orderedList.addAll(wss);
			
		}
		
		return orderedList;
	}


	private void scheduleTopologyUsingRoundRobin(TopologyDetails topology, List<WorkerSlot> availableSlots, Cluster cluster){

		String topologyId = topology.getId();
		
		/* If current topology is migrating, do not reschedule it */
		if (isMigrating(topologyId)){
			System.out.println("Topology " + topologyId + " is migrating, no action taken");
			return;
		}
		
		/* DEBUG */
		System.out.println("Available Slots. ");
		System.out.println(availableSlots);	
		

		/* List of executor to schedule */
		Collection<ExecutorDetails> allExecutors = topology.getExecutors();
		Set<ExecutorDetails> aliveAndAssignedExecutors = new HashSet<ExecutorDetails>();
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
		if (existingAssignment != null){
			aliveAndAssignedExecutors = existingAssignment.getExecutors();
		}

		/* Don't reschedule executors correctly running */
		Set<ExecutorDetails> executorsToReassign = new HashSet<ExecutorDetails>(allExecutors);
		executorsToReassign.removeAll(aliveAndAssignedExecutors);

		/* Determine the number of executors per slot */
		int executors = allExecutors.size();
		int slotToUse = Math.min(topology.getNumWorkers(), availableSlots.size());
		int executorsPerSlot = (int) Math.ceil((double) executors / (double) slotToUse);

		/* DEBUG */
		System.out.println("Executors: " + executors);
		System.out.println("Executors to reassing: " + executorsToReassign.size());
		System.out.println("Slot To Use: " + slotToUse);
		System.out.println("Executors per Slot: " + executorsPerSlot);
		
		
		Iterator<ExecutorDetails> executorsIt = executorsToReassign.iterator();
		for (int i = 0; i < slotToUse; i++){
		
			int k = 0;
			Set<ExecutorDetails> executorsGroup = new HashSet<ExecutorDetails>();
			
			while (executorsIt.hasNext() && k < executorsPerSlot){
				ExecutorDetails executor = executorsIt.next();
				executorsGroup.add(executor);
				k++;
			}

			if (executorsGroup.isEmpty()){
				System.out.println("> All executors have been assigned");
				break;
			}
			
			WorkerSlot ws = availableSlots.get(0);
			System.out.println(" > Assigning " + executorsGroup.size() + " executors to " + ws.getNodeId() + ":" + ws.getPort());
			for(ExecutorDetails e : executorsGroup){
				System.out.println(" > > " + e);
			}

			cluster.assign(ws, topologyId, executorsGroup);
			availableSlots.remove(0);
			
			if (!executorsIt.hasNext()){
				System.out.println("All executors have been assigned");
				break;
			}
		}
	}
	
	
	
	private void scheduleTopology(TopologyDetails topology, GeneralTopologyContext topologyContext, 
			List<WorkerSlot> availableSlots, Cluster cluster, Map<String, Node> knownNetworkSpaceNodes){

		String topologyId = topology.getId();
		
		/* If current topology is migrating, do not reschedule it */
		if (isMigrating(topologyId)){
			System.out.println("Topology " + topologyId + " is migrating, no action taken");
			return;
		}
		

		/* DEBUG */
		/* DEBUG */
		System.out.println(" ** Nodes ********************************************************* ");
		for(String nid : knownNetworkSpaceNodes.keySet()){
			Node n = knownNetworkSpaceNodes.get(nid);
			System.out.println(n);
		}
		System.out.println(" ** Task To Component ********************************************* ");
		Map<Integer, String> taskToComponent = topologyContext.getTaskToComponent();
		for(Integer t : taskToComponent.keySet()){
			String componentId = taskToComponent.get(t);
			System.out.println(t + " -> " + componentId);
		}
		System.out.println(" ****************************************************************** ");

		
		System.out.println("Calculate Executor Pools");
		/* 1. Calculate executor pools */
		List<ExecutorPool> executorPools = calculateExecutorPools(topologyId, topology, topologyContext, cluster, availableSlots, knownNetworkSpaceNodes);
		boolean schedulingNeeded = false;
		for(ExecutorPool ep : executorPools){
			if (!ep.isAssigned()){
				schedulingNeeded = true;
				break;
			}
		}
		if (!schedulingNeeded)
			return;
		
		/* 2. Determinate relations between Executor Pools */
		System.out.println("Determinate relations between Executor Pools");
		determineRelationsBetweenExecutorPools(executorPools, topologyContext);
		
		/* 3. Determinate pinned Executor Pools */
		System.out.println("Determinate pinned Executor Pools");
		determinePinnedExecutorPools(executorPools);
		
		/* DEBUG */
		System.out.println(" --- Total number of executor pools: " + executorPools.size());
		for(int i = 0; i < executorPools.size(); i++){
			ExecutorPool ep = executorPools.get(i);
			System.out.println(" EP[" + i + "] P: " + ep.isPinned() + " A: " + ep.isAssigned() + " S: " + ep.isContainsSources() + " T: " + ep.isContainsTargets() + ":: " + ep.getExecutors());
			System.out.println(" > Parents: " + ep.getParentExecutorPools());
			System.out.println(" > Child: " + ep.getChildExecutorPools());
		}
		System.out.println(" --- --- --- --- --- --- --- --- --- --- ");
		

		/* 4. Assign pinned Executor Pools */
		System.out.println("Assign pinned Executor Pools");
		assignPinnedExecutorPools(topologyId, executorPools, availableSlots, cluster, knownNetworkSpaceNodes);
		
		/* 5. Initialize unpinned Executor Pools */
		System.out.println("Initialize unpinned Executor Pools");
		initializeUnpinnedExecutorPools(executorPools, knownNetworkSpaceNodes);

		/* 6. Assign unpinned Executor Pools */
		System.out.println("Assign unpinned Executor Pools");
		assignUnpinnedExecutorPools(topologyId, executorPools, availableSlots, cluster, knownNetworkSpaceNodes);

		System.out.println(" --- Final EP Coordinates ");
		for(int i = 0; i < executorPools.size(); i++){
			ExecutorPool ep = executorPools.get(i);
			System.out.println(" EP[" + i + "] P: " + ep.isPinned() + " A: " + ep.isAssigned() + " WS: " + ep.getWorkerSlot());
		}

		
	}

	private void assignPinnedExecutorPools(String topologyId, List<ExecutorPool> executorPools, 
			List<WorkerSlot> availableSlots, Cluster cluster, Map<String, Node> knownNetworkSpaceNodes){
		
		for(ExecutorPool ep : executorPools){
			
			if (ep.isPinned() && !ep.isAssigned()){
				if (availableSlots.size() > 0){
					WorkerSlot ws = availableSlots.get(0);
					
					/* Assign Executor Pool to a worker slot */
					cluster.assign(ws, topologyId, ep.getExecutors());
					
					System.out.println("... EP-pinned " + ep + " assigned to " + ws.getNodeId() + ":" + ws.getPort());
					
					/* Update available slots */
					availableSlots.remove(0);
					
					/* Update Executor Pool */
					Node node = getWorkerSlotCoordinates(knownNetworkSpaceNodes, ws);
					ep.setWorkerSlot(ws);
					ep.setAssigned(true);
					ep.setPosition(node);
				}
			}
		}
	}
	
	private void assignUnpinnedExecutorPools(String topologyId, List<ExecutorPool> executorPools, 
			List<WorkerSlot> availableSlots, Cluster cluster, Map<String, Node> knownNetworkSpaceNodes){
		
		boolean updated = true;
		
		while(updated){
			updated = false;
			
			List<WorkerSlot> availableSlotPerRound = new ArrayList<WorkerSlot>(availableSlots);
			
			System.out.println("Available Slot per round: " + availableSlotPerRound.size());
			System.out.println("Executor pools: " + executorPools.size());

			if (availableSlotPerRound.isEmpty()){
				System.out.println("There are no available slot for current Executor Pool");
				return;
			}
			
			for(ExecutorPool ep : executorPools){
				
				if (!ep.isAssigned()){
					/* 1. calculate new position into the network space */
					System.out.println("-- Compute new position");
					Point newCoordinates = computeNewPosition(ep);
					
					/* 2. map new position into a physical worker slot */
					System.out.println("-- Compute new position");
					WorkerSlot ws = retrieveNearestNode(newCoordinates, ep, topologyId, availableSlotPerRound, knownNetworkSpaceNodes);
					
					if (ws == null){
						System.out.println("There are no available slot for current Executor Pool");
						/* XXX: this point should be modified; scheduler should check if another configuration is available */
						return; 
					}
					
					if (ep.getWorkerSlot() == null || !ep.getWorkerSlot().equals(ws)){
						System.out.println("-- Compute new position: updated");
						updated = true;
					}
					
					/* Update Executor Pool with round informations */
					System.out.println("-- Setting new worker slot");
					ep.setWorkerSlot(ws);
					ep.setPosition(getWorkerSlotCoordinates(knownNetworkSpaceNodes, ws));

					/* Update available slots */
					System.out.println("-- Updating available slot per round: " + availableSlotPerRound.size());
					availableSlotPerRound.remove(ws);
				}
			}
		}

		System.out.println("Assigning EP to WS");
		
		/* Assign EP to WS */
		for(ExecutorPool ep : executorPools){
			
			if (!ep.isAssigned()){
				WorkerSlot ws = ep.getWorkerSlot();
				
				if (ws == null)
					continue;
				
				/* Assign Executor Pool to a worker slot */
				cluster.assign(ws, topologyId, ep.getExecutors());
				
				System.out.println("... EP-unpinned " + ep + " assigned to " + ws.getNodeId() + ":" + ws.getPort());
				
				/* Update available slots */
				availableSlots.remove(ws);
				
				/* Update Executor Pool */
				Node node = getWorkerSlotCoordinates(knownNetworkSpaceNodes, ws);
				ep.setWorkerSlot(ws);
				ep.setAssigned(true);
				ep.setPosition(node);
				
			}
		}
	}
	

	private Point computeNewPosition(ExecutorPool executorPool){
		
		@SuppressWarnings("unused")
		int statCounter = 0;
		
		int forcedExit = 10000;
		
		SpringForce f = null;
		Point epNewCoordinates = null;
		if (executorPool.getPosition() == null || executorPool.getPosition().getCoordinates() == null){
			epNewCoordinates = new Point(networkSpace.getTotalDimensions());
		}else{
			epNewCoordinates = executorPool.getPosition().getCoordinates();
		}

		if(DEBUG_RELAXATION_PLACEMENT)
			System.out.println(" ==== Executor " + executorPool + " coordinates: " + epNewCoordinates);
		
		do{
			/* 1. Set spring force to 0 */
			f = new SpringForce();

			/* 2. For each parent component (in every worker slot) compute and sum the spring force (spring force = latency) */
//			System.out.println(" ==== Parent Components");
			SpringForce parentComponentsForce = computeComponentsForce(epNewCoordinates, executorPool.getParentExecutorPools());
			if (!parentComponentsForce.isNull())
				f.add(parentComponentsForce);
			
			/* 3. For each child component (in every worker slot) compute and sum the spring force  */
//			System.out.println(" ==== Child Components");
			SpringForce childComponentsForce = computeComponentsForce(epNewCoordinates, executorPool.getChildExecutorPools());
			if (!childComponentsForce.isNull())
				f.add(childComponentsForce);
			
			/* 4.5 update executor position */
			if(DEBUG_RELAXATION_PLACEMENT)
				System.out.println(" ==== Total force: " + f);
			epNewCoordinates =  f.movePoint(epNewCoordinates, SPRING_FORCE_DELTA);
			
			statCounter++;
			forcedExit--;
		}while(f != null && !f.lessThan(SPRING_FORCE_THRESHOLD) && forcedExit > 0);
		
		if(DEBUG_RELAXATION_PLACEMENT)
			System.out.println(" ==== Final position into the network space: " + epNewCoordinates + " (iterations: " + statCounter +")");
		
		return epNewCoordinates;

	}

	
	
	private SpringForce computeComponentsForce(Point myCoordinates, List<ExecutorPool> relatedExecutorPools){
		
		SpringForce f = new SpringForce();
		if (relatedExecutorPools == null)
			return null;
		
		for(ExecutorPool otherEp : relatedExecutorPools){
			
			Node otherEpNetSpacePosition = otherEp.getPosition();
			if (otherEpNetSpacePosition == null)
				continue;
			SpringForce cf = new SpringForce(otherEpNetSpacePosition.getCoordinates(), myCoordinates);
			f.add(cf);
			
		}		
		return f;		
	}
	
	
	private WorkerSlot retrieveNearestNode(Point newExecutorPoolPosition, ExecutorPool executorPool, 
			String topologyId, List<WorkerSlot> availableSlot, Map<String, Node> networkSpaceNodes){
		
		WorkerSlot candidateSlot = null; 
		if (newExecutorPoolPosition == null){
			return null;
		}
		
		List<KNNItem> kNearestNode = getKNearestNodes(K_NEAREST_NODE_TO_RETRIEVE, newExecutorPoolPosition, networkSpaceNodes);
		
		/* Check returned list */
		if (kNearestNode == null || kNearestNode.size() == 0){
			return null;
		}

        for (KNNItem item : kNearestNode){

        	Node n = item.getNode();
        	
        	if (n == null)
        		continue;
        	
        	String candidateId = n.getSupervisorId();
    		System.out.println("Candidate: ID: " + candidateId);
    		
			/* Find available slot on nearest node */
		    List<WorkerSlot> availableSlotsOnCandidateNode = getAvailableSlotOnCandidateNode(candidateId, availableSlot);

        	System.out.println("Available slots: " + availableSlotsOnCandidateNode);
        	
		    if (availableSlotsOnCandidateNode == null || availableSlotsOnCandidateNode.isEmpty()){
		    	System.out.println("There are no available slots on supervisor ... ");
		    	continue;
		    } else {
		    	
		    	candidateSlot = availableSlotsOnCandidateNode.get(0);
		    	if (candidateSlot == null){
		    		System.out.println("Candidate slot is null");
		    		continue;
		    	} else {
		    		return candidateSlot;
		    	}
		    }    
        
		}
        
        return candidateSlot;
	}
	
	private List<WorkerSlot> getAvailableSlotOnCandidateNode(String candidateNode, List<WorkerSlot> availableSlots){
		
		List<WorkerSlot> wsOnCandidate = new ArrayList<WorkerSlot>();
		
		for (WorkerSlot ws : availableSlots){
			
			if (ws.getNodeId().equals(candidateNode))
				wsOnCandidate.add(ws);
			
		}
		
		return wsOnCandidate;
	}
	
	private List<KNNItem> getKNearestNodes(int k, Point position, Map<String, Node> networkSpaceNodes){
		
		KNearestNodes knn = new SimpleKNearestNodes(this.networkSpace);

		return knn.getKNearestNode(k, position, networkSpaceNodes);
		
	}
	
	
	private void initializeUnpinnedExecutorPools(List<ExecutorPool> executorPools, Map<String, Node> networkSpaceNodes){
		
		for(ExecutorPool ep : executorPools){
			if (!ep.isPinned()){
				if(!ep.isAssigned()){
					ep.setPosition(new Node(networkSpace.getTotalDimensions(), NIMBUS_STUB_NODE_ID));
				}else{
					if (ep.getWorkerSlot() != null && ep.getPosition()==null){
						Node n = getWorkerSlotCoordinates(networkSpaceNodes, ep.getWorkerSlot());
						ep.setPosition(n);

					}
				}
			}
			
		}
	}

	
	private void determinePinnedExecutorPools(List<ExecutorPool> executorPools){
		
		for(ExecutorPool ep : executorPools){
			if (ep.isContainsSources() || ep.isContainsTargets()){
				ep.setPinned(true);
			}
		}
		
		return;
	}

	private void determineRelationsBetweenExecutorPools(List<ExecutorPool> executorPools, GeneralTopologyContext context){
		
		for(ExecutorPool ep : executorPools){
			
			Set<String> components = ep.getComponents();
			Set<String> childComponents = new HashSet<String>();
			Set<String> parentComponents = new HashSet<String>();
			
			boolean containsSources = false;
			boolean containsTargets = false;
			
			for(String componentId : components){
				List<String> targets = getTargetComponentsId(context, componentId);
				List<String> sources = getSourceComponentsId(context, componentId);
				
				/* If it's a system component, do not check for other related system components */
				if (!componentId.startsWith("__")){
					if (!containsSources)
						containsSources = areAllSystemComponents(sources);
					if (!containsTargets)
						containsTargets = areAllSystemComponents(targets);
				}
				
				childComponents.addAll(targets);
				parentComponents.addAll(sources);
			}
			
			ep.setChildComponents(childComponents);
			ep.setParentComponents(parentComponents);

			List<ExecutorPool> parentEp = new ArrayList<ExecutorPool>();
			List<ExecutorPool> childEp = new ArrayList<ExecutorPool>();
			
			for(ExecutorPool otherEp : executorPools){
				for(String childComponentId : childComponents){
					if (otherEp.getComponents() != null && otherEp.getComponents().contains(childComponentId)){
						childEp.add(otherEp);
						break;
					}
				}
				for(String parentComponentId : parentComponents){
					if (otherEp.getComponents() != null && otherEp.getComponents().contains(parentComponentId)){
						parentEp.add(otherEp);
						break;
					}
				}
			}
			
			ep.setParentExecutorPools(parentEp);
			ep.setChildExecutorPools(childEp);
			
			ep.setContainsSources(containsSources);
			ep.setContainsTargets(containsTargets);

		}
		
	}
	
	private List<String> getTargetComponentsId(GeneralTopologyContext context, String componentId){
		
		List<String> targetComponentsId = new ArrayList<String>();
		
		Map<String, Map<String, Grouping>> targets = context.getTargets(componentId);
		
		for(String streamId : targets.keySet()){
			Set<String> componentsId = targets.get(streamId).keySet();

			if (streamId!=null && streamId.equals("default"))
				targetComponentsId.addAll(componentsId);
		}
		
		return targetComponentsId;
	}
	
	private List<String> getSourceComponentsId(GeneralTopologyContext context, String componentId){
		
		List<String> sourceComponentsId = new ArrayList<String>();

		Map<GlobalStreamId, Grouping> sources = context.getSources(componentId);
		for(GlobalStreamId sid : sources.keySet()){
			if (sid!=null && !sid.get_componentId().startsWith("__"))
				sourceComponentsId.add(sid.get_componentId());
		}
		
		return sourceComponentsId;

	}
	
	private boolean areAllSystemComponents(List<String> componentsId){
		boolean system = true;
		for (String c : componentsId){
			if (!c.startsWith("__")){
				system = false;
			}
		}
		return system;
	}
	
	private List<ExecutorPool> calculateExecutorPools(String topologyId, TopologyDetails topology, 
			GeneralTopologyContext topologyContext, Cluster cluster, List<WorkerSlot> availableSlots, Map<String, Node> knownNetworkSpaceNodes){
	
		/* Determine the set of executor pools */
		/*
		 * 1. determine the number of executor pool to create
		 * 2. determine if there is some executor already placed, retrieve existing executor pools
		 * 3. determine new executor pools (total - already placed, if 0 return 1)
		 * 
		 */
		
		/* 1. determine the number of executor pool to create */
		/* List of executors: all, assigned and not assigned */
		Collection<ExecutorDetails> allExecutors = topology.getExecutors();
		Set<ExecutorDetails> aliveAndAssignedExecutors = new HashSet<ExecutorDetails>();
		Set<ExecutorDetails> unassignedExecutors = new HashSet<ExecutorDetails>(allExecutors);		
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
		if (existingAssignment != null){
			aliveAndAssignedExecutors = existingAssignment.getExecutors();
		}
		unassignedExecutors.removeAll(aliveAndAssignedExecutors);

		/* Slot to use is equal to the number of executor pools to create */
		int slotToUse = Math.min(topology.getNumWorkers(), availableSlots.size());
		int numExecutorPerSlot = (int) Math.ceil((double) allExecutors.size() / (double) slotToUse);

		/* DEBUG */
		System.out.println(". Executors: " + allExecutors.size());
		System.out.println(". Unassigned Executors: " + unassignedExecutors.size());
		System.out.println(". Available Slots: " + availableSlots.size());
		System.out.println(". Slot To Use: " + slotToUse + ", executors/slot: " + numExecutorPerSlot);
		

		/* 2. determine if there is some executor already placed, retrieve existing executor pools */
		List<ExecutorPool> executorPools = new ArrayList<ExecutorPool>();
		if (aliveAndAssignedExecutors.size() > 0){
			executorPools = getAssignedExecutorPools(aliveAndAssignedExecutors, existingAssignment, topologyContext, knownNetworkSpaceNodes);
		}
		System.out.println(". Existent Pools: " + executorPools.size());
		for(ExecutorPool ep : executorPools){
			System.out.println("  + " + ep.getWorkerSlot() + " : num exec: " + ep.getExecutors().size());
		}
		
		/* 3. determine new executor pools (total - already placed, if 0 return 1) */
		int newPoolsToCreate = slotToUse - executorPools.size();
		if (newPoolsToCreate <= 0)
			newPoolsToCreate = 1;
		numExecutorPerSlot = (int) Math.ceil((double) unassignedExecutors.size() / (double) newPoolsToCreate);
		System.out.println(". . New executor pools to create: " + newPoolsToCreate + ", executors/slot: " + numExecutorPerSlot);

		if (unassignedExecutors.size() == 0){
			/* If schedule() has been called for current topology, the number of worker used are lower than required */
			
			System.out.println(". . Missing requirement: required numWorkers: " + topology.getNumWorkers() + " - effective workers: "  + executorPools.size());
			
			if (availableSlots.size() == 0){
				System.out.println("There are no available slot!");
				return executorPools;
			}
			
			newPoolsToCreate = topology.getNumWorkers() - executorPools.size();
			newPoolsToCreate = Math.min(newPoolsToCreate, availableSlots.size());
			System.out.println(". . New pools to create: "  + newPoolsToCreate);
			
			for(int i = 0; i < newPoolsToCreate; i++){
				
				/* 1. get biggest executor pool */
				ExecutorPool biggestEP = getBiggestExecutorPool(executorPools);
				
				/* 2. free its slot  */
				if(biggestEP.isAssigned()){
					WorkerSlot ws = biggestEP.getWorkerSlot();
					cluster.freeSlot(ws);
					availableSlots.add(ws);
				}
				
				/* 3. remove selected ep from executorPools */
				executorPools.remove(biggestEP);
				
				/* 4. split executor pool into two executor pool */
				List<ExecutorPool> splittedEP = splitExecutorPools(biggestEP, topologyContext);
				
				System.out.println(". . Splitted EP: "  + splittedEP);
				for(ExecutorPool ep : splittedEP){
					System.out.println(". . . "  + ep.getExecutors());
				}
				
				/* 5. add new ep to executorPools */
				executorPools.addAll(splittedEP);
			}
			
		} else {
			/* One or more executor unassigned (new scheduling or re-scheduling) */
			
			Iterator<ExecutorDetails> executorsIt = unassignedExecutors.iterator();
			for (int i = 0; i < newPoolsToCreate; i++){
			
				int k = 0;
				Set<ExecutorDetails> executorsGroup = new HashSet<ExecutorDetails>();
				Set<String> componentIds = new HashSet<String>();

				while (executorsIt.hasNext() && k < numExecutorPerSlot){
					ExecutorDetails executor = executorsIt.next();
					executorsGroup.add(executor);
					
					if (executor != null){
						String componentId = topologyContext.getComponentId(executor.getStartTask());
						componentIds.add(componentId);
					}
					
					k++;
				}

				if (executorsGroup.isEmpty()){
//					System.out.println("> All executors have been assigned");
					break;
				}
				
				ExecutorPool ep = new ExecutorPool();
				ep.setExecutors(executorsGroup);
				ep.setAssigned(false);
				ep.setPinned(false);
				ep.setComponents(componentIds);			
				
				executorPools.add(ep);
				
				if (!executorsIt.hasNext()){
//					System.out.println("All executors have been assigned");
					break;
				}
			}
		}
		
		

		
		return executorPools;
		
	}
	
	
	private List<ExecutorPool> getAssignedExecutorPools(Set<ExecutorDetails> assignedExecutors, 
			SchedulerAssignment assignment, GeneralTopologyContext context, 
			Map<String, Node> knownNetworkSpaceNodes){
		
		List<ExecutorPool> pool = new ArrayList<ExecutorPool>();
		
		if (assignment == null || context == null)
			return pool;
		
		Map<WorkerSlot, List<ExecutorDetails>> workerToExecutors = new HashMap<WorkerSlot, List<ExecutorDetails>>();
		Map<ExecutorDetails, WorkerSlot> executorToSlot = assignment.getExecutorToSlot();
		
		if (executorToSlot == null || executorToSlot.isEmpty())
			return pool;
		
		/* Create map workerSlot -> executors */
		for (ExecutorDetails e : assignedExecutors){
			if (e == null){
				System.out.println("WARNING: invalid executor");
				continue;
			}
			
			WorkerSlot ws = executorToSlot.get(e);
			if (ws == null){
				System.out.println("WARNING: executor" + e + " has NOT a worker slot");
				continue;
			}
			
			List<ExecutorDetails> list = workerToExecutors.get(ws);
			if (list == null)
				list = new ArrayList<ExecutorDetails>();
			list.add(e);
			
			workerToExecutors.put(ws, list);
		}
		
	
	
		/* Create executor pools */
		for(WorkerSlot ws : workerToExecutors.keySet()){
			
			List<ExecutorDetails> executors = workerToExecutors.get(ws);
			
			Set<String> componentIds = new HashSet<String>();
			for (ExecutorDetails e : executors){
				if (e != null){
					componentIds.add(context.getComponentId(e.getStartTask()));
				}
			}

			Node node = getWorkerSlotCoordinates(knownNetworkSpaceNodes, ws);

			ExecutorPool ep = new ExecutorPool();
			ep.setWorkerSlot(ws);
			ep.setExecutors(new HashSet<ExecutorDetails>(executors));
			ep.setPinned(false);
			ep.setAssigned(true);
			ep.setComponents(componentIds);
			ep.setPosition(node);
			
			pool.add(ep);
			
		}
		
		return pool;
		
	}
	
	private Node getWorkerSlotCoordinates(Map<String, Node> knownNetworkSpaceNodes, WorkerSlot workerSlot){

		/* 
		 * Retrieve node position following the steps: 
		 *  - search between nodes known by the network space manager
		 *  - if unknown, query zookeeper
		 *  - if unknown, return null 
		 * */
	
		if (workerSlot == null)
			return null;
		
		String nodeId = workerSlot.getNodeId();
		
		Node coordinates = knownNetworkSpaceNodes.get(nodeId);
		
		if (coordinates == null){
//			System.out.println("+++ coordinates not known, retriving from zookeeper...");
			coordinates = retrieveCoordinatesFromZK(nodeId);
			if (coordinates != null)
				knownNetworkSpaceNodes.put(coordinates.getSupervisorId(), coordinates);
		}

//		System.out.println("Coordinates: " + coordinates);
		return coordinates;
	}
	
	private boolean isMigrating(String topologyId){
		String dirname = ZK_MIGRATION_DIR + "/" + topologyId;
		
		if (zkClient.exists(dirname)){
			
			List<String> children = zkClient.getChildren(dirname);
			
			if (children == null || children.isEmpty()){
				return false;
			}
			
			return true;
		} else {
			return false;
		}
	}
	
	private Node retrieveCoordinatesFromZK(String nodeId){
		
		String path = ZK_COORDINATES_DIR + "/" + nodeId;

		byte[] data = zkClient.getData(path);
		
		if (data == null)
			return null;
		else
			return Serializer.deserializeCoordinates(nodeId, data);
	}
	
	private ExecutorPool getBiggestExecutorPool(List<ExecutorPool> executorPools){
		
		ExecutorPool biggest = null;
		int biggestSize = Integer.MIN_VALUE;
		
		for(ExecutorPool ep : executorPools){
			
			if (ep.getExecutors()!= null && ep.getExecutors().size() > biggestSize){
				biggest = ep;
				biggestSize = ep.getExecutors().size();
			}
			
		}
		
		return biggest;
		
	}
	

	private List<ExecutorPool> splitExecutorPools(ExecutorPool executorPool, GeneralTopologyContext context){
		
		if (executorPool.getExecutors() == null)
			return null;
		
		List<ExecutorPool> pool = new ArrayList<ExecutorPool>();
		
		int originalSize = executorPool.getExecutors().size();
		int firstExecutorPoolSize = (int) Math.ceil((double) originalSize / 2.0);
		
		/* Create set #1 */
		Set<ExecutorDetails> firstSet = new HashSet<ExecutorDetails>();
		Iterator<ExecutorDetails> it = executorPool.getExecutors().iterator();
		for(int i = 0; i < firstExecutorPoolSize; i++){
			if (it.hasNext()){
				firstSet.add(it.next());
			}else{
				break;
			}
			
		}

		Set<String> componentIds = new HashSet<String>();
		for (ExecutorDetails e : firstSet){
			if (e != null){
				componentIds.add(context.getComponentId(e.getStartTask()));
			}
		}

		ExecutorPool ep = new ExecutorPool();
		ep.setExecutors(firstSet);
		ep.setAssigned(false);
		ep.setPinned(false);
		ep.setComponents(componentIds);			
		pool.add(ep);

		
		
		/* Create set #2 */
		Set<ExecutorDetails> secondSet = new HashSet<ExecutorDetails>(executorPool.getExecutors());
		secondSet.removeAll(firstSet);
		componentIds = new HashSet<String>();
		for (ExecutorDetails e : firstSet){
			if (e != null){
				componentIds.add(context.getComponentId(e.getStartTask()));
			}
		}
		ep = new ExecutorPool();
		ep.setExecutors(secondSet);
		ep.setAssigned(false);
		ep.setPinned(false);
		ep.setComponents(componentIds);			
		pool.add(ep);
		

		return pool;
	}
	
	
	
}
