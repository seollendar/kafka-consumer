
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerWithMultiThread {
	private static String TOPIC_NAME = "test-bash";
	private static String GROUP_ID = "test-group3";
	private static String BOOTSTRAP_SERVERS;
	private static int CONSUMER_COUNT = 5;
	private static List<ConsumerWorker> workerThreads = new ArrayList<ConsumerWorker>();
	
	
	
	public static void main(String[] args) {

		BOOTSTRAP_SERVERS = "localhost:9092"; 
		Runtime.getRuntime().addShutdownHook(new ShutdownThread());
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		//configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		//configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		ExecutorService executorService = Executors.newCachedThreadPool();
		for (int i = 0; i < CONSUMER_COUNT; i++) {
			ConsumerWorker worker = new ConsumerWorker(configs, TOPIC_NAME, i); //ConsumerWorkerClassifier
			workerThreads.add(worker);
			executorService.execute(worker);

		}
	}

	static class ShutdownThread extends Thread {
		public void run() {
			System.out.println("shut down");
		}
	}
}
