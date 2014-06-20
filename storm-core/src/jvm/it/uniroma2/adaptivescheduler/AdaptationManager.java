package it.uniroma2.adaptivescheduler;

import java.util.concurrent.locks.ReadWriteLock;

import backtype.storm.scheduler.ISupervisor;

public class AdaptationManager {
	
	private ISupervisor supervisor;
	private ReadWriteLock readWriteLock;
	
	
	
	public AdaptationManager() {
	}
	
	public AdaptationManager(ISupervisor supervisor, ReadWriteLock readWriteLock) {
		super();
		this.supervisor = supervisor;
		this.readWriteLock = readWriteLock;
		System.out.println("Adaptation Manager created!");
	}

	/**
	 * Start the adaptation manager. 
	 * 
	 * This function is called from supervisor.clj/mk-supervisor in a synchronous way. 
	 * A fast start-up should be performed, deferring slow operations in a dedicated thread. 
	 */
	public void start(){
		
		while (true){
			try {
				Thread.sleep(1000);
				System.out.println("Adaptation Manager is alive and it's blocking other the supervisor");
				System.out.println("Supervisor id: " + supervisor.getSupervisorId());
				System.out.println("RWLock: " + readWriteLock);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void stop(){
		
		//wait run to stop
		
	}
	
}
