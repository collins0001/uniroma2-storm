package it.uniroma2.adaptivescheduler.scheduler.builder;

import it.uniroma2.adaptivescheduler.scheduler.IAdaptiveScheduler;

import java.util.Map;

/**
 * Created by Gabriele Scolastri on 06/10/15.
 */
public interface ISchedulerBuilder {

    IAdaptiveScheduler buildScheduler(Object ... params);
}
