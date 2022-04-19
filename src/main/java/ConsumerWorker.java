import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerWorker implements Runnable {

	private Properties prop;
	private String topic;
	private String threadName;
	private KafkaConsumer<String, String> consumer;

	ConsumerWorker(Properties prop, String topic, int number) {
		this.prop = prop;
		this.topic = topic;
		this.threadName = "consumer-thread-" + number;
	}


	public void run() {		


		try {
			consumer = new KafkaConsumer<String, String>(prop);
			consumer.subscribe(Arrays.asList(topic));
			//consumer.offsetsForTimes(timestampsToSearch);
			System.out.println("== classify Module == "+ threadName +" ==");


			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(1000);

				for (ConsumerRecord<String, String> record : records) {
					try {
						System.out.println(record);
					}catch(Exception e){
						System.out.println("record Exception raised: " + e );
						continue;
					}

				}//for

			}//while

		}catch(Exception e){
			System.out.println("consumer Exception raised: " + e);
		} finally {
			run();
		}		

	}

	public void shutdown() {
		consumer.wakeup();
	}


}
