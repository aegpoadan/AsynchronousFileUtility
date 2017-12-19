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
import java.util.concurrent.ScheduledExecutorService;
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
public final class FileUtil {
	private static final Logger logger;
	private static final String threadPoolName;
	private static final ScheduledExecutorService asyncFilePool;
	
	static {
		logger = LogManager.getLogger(FileUtil.class);
		threadPoolName = "asyncFilePool";
		asyncFilePool = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
	}
	
	public static final String readFileSync(File file, Charset encoding) throws IOException {
	  byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
	  return new String(encoded, encoding);
	}

	/**
	 * Saves the passed byte array asynchronously to the specified path. 
	 * Once non-blocking operation completes, a callback is executed.
	 * Callback function will receive one argument; the result of the write operation.
	 * @param bytes an array of byte to save
	 * @param path the system path to save the bytes to
	 * @param callback a function to execute after save completion
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
	 * Reads the file at a specified path asynchronously into a byte array.
	 * Once non-blocking operation completes, a callback is executed.
	 * Callback function will receive two arguments; the result of the read operation 
	 * and the read byte array.
	 * @param path the system path to read from
	 * @param callback the callback function to execute after the file is read
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
	
	public static final void close() {
		asyncFilePool.shutdown();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		String test = "Hello world!";
		byte[] bytesToSave = test.getBytes();
		
		saveBytesAsync(bytesToSave, "/Users/hayesap/Downloads/test.txt", (result) -> {
			if(result == bytesToSave.length) {
				System.out.println("File was succesfully saved!");
				
				try {
					readBytesAsync("/Users/hayesap/Downloads/test.txt", (readResult, bytes) -> {
						System.out.println(new String(bytes));
						close();
					});
				} catch (Exception e) {
					logger.error("Failed to read file", e);
					close();
				} 
			}
		});
	}
}
