package AsynchronousFileUtility;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author Andrew Hayes
 * 
 * A Utility class to execute asynchronous tasks.
 */
public final class ExecutorUtil {
	@SuppressWarnings("unused")
	private static final Logger logger;
	static {
		logger = LogManager.getLogger(ExecutorUtil.class);;
	}
	
	/**
	 * Asynchronously executes passed functions repeatedly in parallel, with a fixed delay in-between repeats.
	 * @param delay the delay in seconds to apply between each task completion
	 * @param threadPool the thread pool to use for function execution
	 * @param functions an array of functions to execute
	 */
	public static final void executePeriodicBatchTasks(int delay, ScheduledExecutorService threadPool, Runnable... functions) {
		Arrays.stream(functions).parallel()
		.forEach((function) -> {
			threadPool.scheduleWithFixedDelay(function, 0, delay, TimeUnit.SECONDS);
		});
	}
	
	/**
	 * Asynchronously executes passed functions in parallel.
	 * @param threadPool the thread pool to use for function execution
	 * @param functions an array of functions to execute
	 */
	public static final void executeTasks(ScheduledExecutorService threadPool, Runnable... functions) {
		Arrays.stream(functions).parallel()
		.forEach((function) -> {
			threadPool.execute(function);
		});
	}

}
