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
	public static final void executeTasks(ExecutorService threadPool, Runnable... functions) {
		Arrays.stream(functions).parallel()
		.forEach((function) -> {
			threadPool.execute(function);
		});
	}
	
	public static final Future<?> executeTaskAsync(ExecutorService threadPool, Supplier<?> function) {
		return threadPool.submit(() -> {
			return function.get();
		});
	}
	
	public static final void dispatchTaskAsync(ExecutorService threadPool, Runnable function) {
		threadPool.execute(function);
	}
	
	public static final Type getType(final Class<?> klass, final int pos) {
	    // obtain anonymous, if any, class for 'this' instance
	    final Type superclass = klass.getGenericSuperclass();

	    // test if an anonymous class was employed during the call
	    if ( !(superclass instanceof Class) ) {
	        throw new RuntimeException("This instance should belong to an anonymous class");
	    }

	    // obtain RTTI of all generic parameters
	    final Type[] types = ((ParameterizedType) superclass).getActualTypeArguments();

	    // test if enough generic parameters were passed
	    if ( pos < types.length ) {
	        throw new RuntimeException(String.format(
	           "Could not find generic parameter %d because only %d parameters were passed",
	              pos, types.length));
	    }

	    // return the type descriptor of the requested generic parameter
	    return types[pos];
	}
	
	public static final <T> void executeTaskAsyncCallback(ExecutorService threadPool, Supplier<T> supplier, Consumer<T> callback) {
		threadPool.execute(() -> {
			Thread currentThread = Thread.currentThread();
			
			try {
				T result = threadPool.submit(() -> {
					return supplier.get();
				}).get();
				
				callback.accept(result);
			}  catch (Exception e) {
				logger.error("Failed to execute asynchronous task in ExecutorUtil.executeTaskAsyncCallback(). Current thread: " + currentThread.getName(), e);
			}
		});
	}
}
