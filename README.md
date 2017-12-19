# AsynchronousFileUtility
A collection of functions to make file IO easy in Java and in the style of Javascript. 
Utilizes Java 8 Streams and lambdas to perform read/writes asynchonously and in parallel.

##Usage
'''Java

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
'''
