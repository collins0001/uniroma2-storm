package it.uniroma2.adaptivescheduler.scheduler.builder;

import backtype.storm.scheduler.ISupervisor;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.scheduler.IAdaptiveScheduler;
import it.uniroma2.adaptivescheduler.scheduler.springforce.AdaptiveScheduler;
import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.util.Map;

/**
 * Created by Gabriele Scolastri on 06/10/15.
 */
public class SpringForceBuilder implements ISchedulerBuilder {

    /**
     * Parameters required:
     * - ISupervisor
     * - SimpleZookeeperClient
     * - DatabaseManager
     * - QoSMonitor
     * - Map<String, Object> (i.e. Config mapping)
     *
     * @param params: ISupervisor, SimpleZookeeperClient, DatabaseManager, QoSMonitor, Map<String, Object>
     * @return
     */
    @Override
    public IAdaptiveScheduler buildScheduler(Object... params) {

        ISupervisor sup;
        SimpleZookeeperClient zkc;
        DatabaseManager dbm;
        QoSMonitor qsm;
        Map cfg;

        try {
            sup = (ISupervisor) params[0];
            zkc = (SimpleZookeeperClient) params[1];
            dbm = (DatabaseManager) params[2];
            qsm = (QoSMonitor) params[3];
            cfg = (Map) params[4];
        }
        catch(ClassCastException ex)
        {
            System.out.println("Thrown Exception "+ex.getClass().getSimpleName()+": "+ex.getMessage()+"");
            ex.printStackTrace();
            return null;
        }

        return new AdaptiveScheduler(sup, zkc, dbm, qsm, cfg);
    }
}
