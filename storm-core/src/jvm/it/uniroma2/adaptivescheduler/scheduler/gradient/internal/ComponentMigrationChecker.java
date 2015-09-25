package it.uniroma2.adaptivescheduler.scheduler.gradient.internal;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Gabriele Scolastri on 15/09/15.
 */
public class ComponentMigrationChecker {

    public static Logger LOG = LoggerFactory.getLogger(ComponentMigrationChecker.class);

    private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";

    private List<String> localMigratingComponents = new ArrayList<String>();
    private SimpleZookeeperClient zkClient;

    public ComponentMigrationChecker(SimpleZookeeperClient client)
    {
        zkClient = client;
    }

    public void initialize()
    {
        // initialize zookeeper;

        if (zkClient != null){
            try {
                zkClient.mkdirs(ZK_MIGRATION_DIR);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public boolean isMigrating(String topologyID, String componentID)
    {
        String dirname = ZK_MIGRATION_DIR + "/" + topologyID + "/" + componentID;

        boolean zkMigration = zkClient.exists(dirname);

        if (zkMigration){
            for(String localMigration : localMigratingComponents){
				/* If component is migrating by current node, don't consider its migration*/
                if (localMigration.equals(toMigrationID(topologyID, componentID))){
                    return false;
                }
            }

            return true;
        }else{
            return false;
        }
    }

    /* XXX: we should save the inode indicating {component, worker node id}, to guarantee no to "starve" if
	 * a worker node fails and an executor needs to be moved */
    public void setMigration(String stormId, String componentId){
;
        String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;
        String migrationId = toMigrationID(stormId, componentId);
        System.out.println("[GRADIENT] [TOPOLOGY] SET migration: " + migrationId);
        try {
            zkClient.mkdirs(dirname);

            localMigratingComponents.add(migrationId);
        } catch (KeeperException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unsetMigration(String stormId, String componentId){

        String migrationId = toMigrationID(stormId, componentId);
        System.out.println("[GRADIENT] [TOPOLOGY] UNSET migration: " + migrationId);

        String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;

        zkClient.deleteRecursive(dirname);

        localMigratingComponents.remove(migrationId);
    }

    private String toMigrationID(String topologyID, String componentID)
    {
        return topologyID + ":::" + componentID;
    }

}
