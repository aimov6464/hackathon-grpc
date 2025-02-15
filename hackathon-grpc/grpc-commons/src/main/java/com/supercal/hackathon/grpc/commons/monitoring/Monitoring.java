package com.supercal.hackathon.grpc.commons.monitoring;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;

public class Monitoring {

    private static final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    public static int getCpu() {
        // Get the system load average
        double systemLoadAverage = osBean.getSystemLoadAverage();

        // Get the number of available processors
        int availableProcessors = osBean.getAvailableProcessors();

        // Normalize the load average as a percentage
        return (int)((systemLoadAverage / availableProcessors) * 100);
    }

    public static int getRam() {
        // Heap memory usage
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        // Non-heap memory usage
        MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

        return (int)((heapMemoryUsage.getUsed() + nonHeapMemoryUsage.getUsed()) / (1024.0 * 1024));
    }
}
