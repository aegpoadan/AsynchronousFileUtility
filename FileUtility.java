package AsynchronousFileUtility;

import static java.lang.Math.toIntExact;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author Andrew Hayes
 *
 * Utility class to simplify IO in Java. Asynchronous file operations are modeled after file IO in NodeJS. 
 */
public final class FileUtility {
	private static final Logger logger;
	private static final ExecutorService asyncFilePool = new ForkJoinPool();
	
	static {
		logger = LogManager.getLogger(FileUtility.class);
	}
	
	public static final String readFileSync(File file, Charset encoding) throws IOException {
	  byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
	  return new String(encoded, encoding);
	}
	
	/**
	 * Convenience method to save a byte[] asynchronously. Returns an Object[] of length 2.
	 * Index 0 contains the future result of the async save (number of bytes written)
	 * Index 1 contains the asynchronous file channel, so that it may be closed 
	 * @param bytes the bytes to save to a file
	 * @param path the system path to save the file to
	 * @return Object[] containing the Future<Integer> result and the file channel
	 * @throws IOException
	 */
	public static final Object[] saveBytesAsync(byte[] bytes, String path) throws IOException {
		Path file = Paths.get(path);
		AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(file, 
				StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);
		
		Future<Integer> pendingResult = asyncFile.write(ByteBuffer.wrap(bytes), 0);
		return new Object[]{pendingResult, asyncFile};
	}
	

	/**
	 * Saves the passed byte array asynchronously to the specified path. 
	 * Once non-blocking operation completes, a callback is executed.
	 * Callback function(s) will receive one argument; the result of the write operation.
	 * If there are multiple callback functions, they will be executed in parallel.
	 * @param bytes an array of byte to save
	 * @param path the system path to save the bytes to
	 * @param callbacks a function to execute after save completion
	 * @throws IOException 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void saveBytesAsync(byte[] bytes, String path, Consumer<Integer> callback) throws IOException, InterruptedException, ExecutionException {
		Path file = Paths.get(path);
		AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(file, 
				StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);
		
		Future<Integer> pendingResult = asyncFile.write(ByteBuffer.wrap(bytes), 0);
		
		ExecutorUtil.executeTasks(asyncFilePool, () -> {
			try {
				Integer result = pendingResult.get();
				asyncFile.close();
				callback.accept(result);
			} catch(Exception e) {
				logger.error("Failed to save file asyncronously in saveBytesAsync()", e);
			} finally {
				if(asyncFile.isOpen()) {
					try {
						asyncFile.close();
					} catch (IOException e) {
						logger.error("Failed to close asyncronous file stream in saveBytesAsync()", e);
					}
				}
			}
		});
	}
	
	/**
	 * Saves the passed byte arrays asynchronously to the specified paths. 
	 * Once non-blocking operation completes, a callback is executed.
	 * Callback function(s) will receive one argument; the result of the write operation.
	 * Each write/callback operation will be executed in parallel.
	 * @param pathsToMap A map containing system paths as keys, and a byte[] to callback pair as values.
	 */
	public static final void saveBytesAsync(Map<String, Map.Entry<byte[], Consumer<Integer>>> pathsToMap) {
		ExecutorUtil.dispatchTaskAsync(asyncFilePool, () -> {
			pathsToMap.entrySet().parallelStream().forEach((entry) -> {
				String path = entry.getKey();
				byte[] bytes = entry.getValue().getKey();
				Consumer<Integer> callback = entry.getValue().getValue();
				
				Path file = Paths.get(path);
				try {
					AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(file, 
							StandardOpenOption.WRITE,
							StandardOpenOption.CREATE);
					
					Future<Integer> pendingResult = asyncFile.write(ByteBuffer.wrap(bytes), 0);
					
					ExecutorUtil.dispatchTaskAsync(asyncFilePool, () -> {
						try {
							Integer result = pendingResult.get();
							asyncFile.close();
							callback.accept(result);
						} catch(Exception e) {
							logger.error("Failed to save file asyncronously in saveBytesAsync()", e);
						} finally {
							if(asyncFile.isOpen()) {
								try {
									asyncFile.close();
								} catch (IOException e) {
									logger.error("Failed to close asyncronous file stream in saveBytesAsync()", e);
								}
							}
						}
					});
				} catch (Exception e) {
					logger.error("Failed to write bytes to file at path: " + path, e);
				}
			});
		});
	}
	
	/**
	 * Reads the file at a specified path asynchronously into a byte array.
	 * Once non-blocking operation completes, a callback(s) is executed.
	 * Callback function(s) will receive two arguments; the result of the read operation 
	 * and the read byte array.
	 * If there are multiple callback functions, they will be executed in parallel.
	 * @param path the system path to read from
	 * @param callbacks the callback function(s) to execute after the file is read
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void readBytesAsync(String path, BiConsumer<Integer, byte[]> callback) throws IOException, InterruptedException, ExecutionException {
		Path file = Paths.get(path);
		if(Files.notExists(file)) {
			IOException e = new IOException("No file at path: " + path);
			logger.error("Attempted to read non-existent file in readBytesAsync()", e);
			throw e;
		}
		long numBytes = file.toFile().length();
		
		AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(file, 
				StandardOpenOption.READ);

		ByteBuffer buffer = ByteBuffer.allocate(toIntExact(numBytes));
		Future<Integer> pendingResult = asyncFile.read(buffer, 0);
		
		ExecutorUtil.executeTasks(asyncFilePool, () -> {
			try {
				Integer result = pendingResult.get();
				asyncFile.close();
				callback.accept(result, buffer.array());
			} catch(Exception e) {
				logger.error("Failed to save file asyncronously in saveBytesAsync()", e);
			} finally {
				if(asyncFile.isOpen()) {
					try {
						asyncFile.close();
					} catch (IOException e) {
						logger.error("Failed to close asyncronous file stream in saveBytesAsync()", e);
					}
				}
			}
		});
	}
	
	/**
	 * Reads the files at specified paths asynchronously into byte arrays.
	 * Once non-blocking operation completes, a callback is executed.
	 * Callback function(s) will receive two arguments; the result of the read operation 
	 * and the read byte array.
	 * Each file read and callback is executed in parallel.
	 * @param pathsToCallbacks A map containing system paths as keys, and callbacks as values
	 */
	public static final void readBytesAsync(Map<String, BiConsumer<Integer, byte[]>> pathsToCallbacks) {
		ExecutorUtil.dispatchTaskAsync(asyncFilePool, () -> {
			pathsToCallbacks.entrySet().parallelStream().forEach((entry) -> {
				String path = entry.getKey();
				BiConsumer<Integer, byte[]> callback = entry.getValue();
				
				Path file = Paths.get(path);
				if(Files.notExists(file)) {
					IOException e = new IOException("No file at path: " + path);
					logger.error("Attempted to read non-existent file in readBytesAsync()", e);
				}
				long numBytes = file.toFile().length();
				
				try { 
					AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(file, 
							StandardOpenOption.READ);
					ByteBuffer buffer = ByteBuffer.allocate(toIntExact(numBytes));
					Future<Integer> pendingResult = asyncFile.read(buffer, 0);
					
					ExecutorUtil.dispatchTaskAsync(asyncFilePool, () -> {
						try {
							Integer result = pendingResult.get();
							asyncFile.close();
							callback.accept(result, buffer.array());
						} catch(Exception e) {
							logger.error("Failed to save file asyncronously in saveBytesAsync()", e);
						} finally {
							if(asyncFile.isOpen()) {
								try {
									asyncFile.close();
								} catch (IOException e) {
									logger.fatal("Failed to close asyncronous file stream in saveBytesAsync()", e);
								}
							}
						}
					});
				} catch(Exception e) {
					logger.error("Failed to read file: " + path + "\nIn FileUtil.readBytesAsync()", e);
				}
			});
		});
	}
	
	public static final void close() {
		asyncFilePool.shutdown();
	}
}
