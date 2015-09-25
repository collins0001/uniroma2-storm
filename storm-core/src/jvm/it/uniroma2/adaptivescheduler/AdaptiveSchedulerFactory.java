package it.uniroma2.adaptivescheduler;

import backtype.storm.Config;
import backtype.storm.scheduler.ISupervisor;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.scheduler.IAdaptiveScheduler;
import it.uniroma2.adaptivescheduler.scheduler.builder.GradientStepBuilder;
import it.uniroma2.adaptivescheduler.scheduler.builder.ISchedulerBuilder;
import it.uniroma2.adaptivescheduler.scheduler.builder.SpringForceBuilder;
import it.uniroma2.adaptivescheduler.scheduler.gradient.GradientStepScheduler;
import it.uniroma2.adaptivescheduler.scheduler.springforce.AdaptiveScheduler;
import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Gabriele Scolastri on 30/09/15.
 */
public abstract class AdaptiveSchedulerFactory {

    public static final String ADAPTIVESCHEDULER_TYPE_GRADIENT_STEP = "GRADIENT_STEP";
    public static final String ADAPTIVESCHEDULER_TYPE_SPRING_FORCE = "SPRING_FORCE";

    public static final String ADAPTIVESCHEDULER_TYPE_DEFAULT = ADAPTIVESCHEDULER_TYPE_SPRING_FORCE;

    public static final Map<String, Class> ADAPTIVESCHEDULER_TAG2BUILDER_MAPPING = getBuilderMapping();

    /**
     * Here it is generated the mapping from tag string to ISchedulerBuilder. In order to add another scheduler,
     * one should:
     * - implement the corresponding ISchedulerBuilder class, preferably under it.uniroma2.adaptivescheduler.scheduler.builder
     * - in this class, add a field ADAPTIVESCHEDULER_TYPE_<NAME_YOUR_SCHEDULER> with a representative string
     * - in this method, update the mapping with your tag-builder class
     *
     * @return the mapping tag-builder class
     */
    private static Map<String, Class> getBuilderMapping()
    {

        Map<String, Class> mapping = new HashMap<String, Class>();

        mapping.put(ADAPTIVESCHEDULER_TYPE_DEFAULT, SpringForceBuilder.class);
        mapping.put(ADAPTIVESCHEDULER_TYPE_SPRING_FORCE, SpringForceBuilder.class);
        mapping.put(ADAPTIVESCHEDULER_TYPE_GRADIENT_STEP, GradientStepBuilder.class);

        return mapping;
    }

    public AdaptiveSchedulerFactory()
    {
        throw new AssertionError("Cannot instantiate " + AdaptiveSchedulerFactory.class.getSimpleName()+ ", use the static method!");
    }

    public static IAdaptiveScheduler getScheduler(Map conf, Object ... params)
    {
        String sValue = (String) conf.get(Config.ADAPTIVE_SCHEDULER_TYPE);
        return getScheduler(sValue, params);
    }

    public static IAdaptiveScheduler getScheduler(String scheduler, Object ... params)
    {

        if(scheduler == null) {
            System.out.println("No adaptive scheduler type specified, using default");
            scheduler = ADAPTIVESCHEDULER_TYPE_DEFAULT;
        }

        System.out.println("Instantiating scheduler: " + scheduler + "");

        ISchedulerBuilder builder = getBuilder(scheduler);

        return builder.buildScheduler(params);

    }

    private static ISchedulerBuilder getBuilder(String scheduler)
    {
        ISchedulerBuilder builder = null;
        Class builderClass = ADAPTIVESCHEDULER_TAG2BUILDER_MAPPING.get(scheduler);
        if(builderClass == null)
            builderClass = ADAPTIVESCHEDULER_TAG2BUILDER_MAPPING.get(ADAPTIVESCHEDULER_TYPE_DEFAULT);

        try
        {
            builder = (ISchedulerBuilder) builderClass.getConstructor(new Class[0]).newInstance(new Object[0]);
        }
        catch(NoSuchMethodException ex)
        {
            ex.printStackTrace();
            return null;
        }
        catch(InstantiationException ex)
        {
            ex.printStackTrace();
            return null;
        }
        catch(IllegalAccessException ex)
        {
            ex.printStackTrace();
            return null;
        }
        catch(InvocationTargetException ex)
        {
            ex.printStackTrace();
            return null;
        }

        return builder;


    }
}
