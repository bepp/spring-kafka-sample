package orcab.sample.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.CoreMatchers.*;

import static org.junit.Assert.*;

import static org.springframework.kafka.test.hamcrest.KafkaMatchers.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:context/applicationContextTest.xml")
public class TransactionTest {

    private KafkaProducer<Integer, String> producer;
    private KafkaConsumer<Integer, String> consumer;
    private TopicPartition topicPartition;

    @Before
    public void setUp() {
        // init Partition
        topicPartition = new TopicPartition("topic1", 0);

        // init Producer
        producer = new KafkaProducer<Integer, String>(createProducerProps(), new IntegerSerializer(),
                new StringSerializer());
        producer.initTransactions();

        // init Consumer
        consumer = new KafkaConsumer<Integer, String>(createConsumerProps(), new IntegerDeserializer(),
                new StringDeserializer());
        consumer.assign(Arrays.asList(topicPartition));
    }

    @After
    public void tearDown() {
        // データがなくなるまでポーリング
        while (!consumer.poll(1000L).isEmpty()) {
        }

        // close Producer/Consumer
        producer.close();
        consumer.close();
    }

    /**
     * Create Producer Config Properties
     *
     * @return properties
     */
    private Properties createProducerProps() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx" + (int) (Math.random() * 100000));
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        return properties;
    }

    /**
     * Create Consumer Config Properties
     *
     * @return properties
     */
    private Properties createConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        return properties;
    }

    /**
     * 【テスト1】トランザクション試験 No.1
     * <p>
     * 以下、2つの観点を試験する。
     * <ul>
     * <li>未コミット状態のメッセージがpollされないこと。</li>
     * <li>コミット状態のメッセージがpollされること。</li>
     * </ul>
     * </p>
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test001() throws InterruptedException, ExecutionException {
        ProducerRecord<Integer, String> message = new ProducerRecord<>("topic1", 0, "test");

        // start Transaction
        producer.beginTransaction();
        producer.send(message).get();

        // 未コミット状態でポーリング
        ConsumerRecords<Integer, String> records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // コミット
        producer.commitTransaction();

        // コミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(false));
        for (ConsumerRecord<Integer, String> record : records) {
            assertThat(record, hasPartition(0));
            assertThat(record, hasKey(0));
            assertThat(record, hasValue("test"));
            System.out.println("polled message: [key] " + record.key() + ", [value] " + record.value());
        }
    }

    /**
     * 【テスト2】トランザクション試験 - アボート
     * <p>
     * 以下、2つの観点を試験する。
     * <ul>
     * <li>未コミット状態のメッセージがpollされないこと。</li>
     * <li>アボート状態のメッセージがpollされないこと。</li>
     * </ul>
     * </p>
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test002() throws InterruptedException, ExecutionException {
        ProducerRecord<Integer, String> message = new ProducerRecord<>("topic1", 0, "test");

        // start Transaction
        producer.beginTransaction();
        producer.send(message).get();

        // 未コミット状態でポーリング
        ConsumerRecords<Integer, String> records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // アボート
        producer.abortTransaction();

        // アボート状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));
    }

    /**
     * 【テスト3】トランザクション試験 - 複数トランザクションのコミット
     * <p>
     * 以下、2つの観点を試験する。
     * <ul>
     * <li>トランザクションが複数になっても適切に管理されること</li>
     * <li>未コミット状態のメッセージがコミット済みメッセージより先にある場合ポールされないこと</li>
     * </ul>
     * </p>
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test003() throws InterruptedException, ExecutionException {
        // init Another Producer
        KafkaProducer<Integer, String> anotherProducer = new KafkaProducer<>(createProducerProps(),
                new IntegerSerializer(), new StringSerializer());
        anotherProducer.initTransactions();

        ProducerRecord<Integer, String> message1 = new ProducerRecord<>("topic1", 1, "test1");
        ProducerRecord<Integer, String> message2 = new ProducerRecord<>("topic1", 2, "test2");

        // start Transaction
        producer.beginTransaction();
        anotherProducer.beginTransaction();

        // anotherProducerでメッセージ送信
        anotherProducer.send(message1).get();

        // anotherProducer未コミット状態でポーリング
        ConsumerRecords<Integer, String> records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // producerでメッセージ送信
        producer.send(message2).get();

        // anotherProducer/producer未コミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // producerコミット
        producer.commitTransaction();

        // producerコミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // anotherProducerコミット
        anotherProducer.commitTransaction();

        // anotherProducerコミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(false));
        for (ConsumerRecord<Integer, String> record : records) {
            assertThat(record, hasPartition(0));
            assertThat(record, hasKey(1));
            assertThat(record, hasValue("test1"));
            System.out.println("polled message: [key] " + record.key() + ", [value] " + record.value());
        }

        // anotherProducerコミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(false));
        for (ConsumerRecord<Integer, String> record : records) {
            assertThat(record, hasPartition(0));
            assertThat(record, hasKey(2));
            assertThat(record, hasValue("test2"));
            System.out.println("polled message: [key] " + record.key() + ", [value] " + record.value());
        }

        // close anotherProducer
        anotherProducer.close();
    }

    /**
     * 【テスト4】トランザクション試験 - 複数トランザクションのアボート
     * <p>
     * 以下、2つの観点を試験する。
     * <ul>
     * <li>トランザクションが複数になっても適切に管理されること。</li>
     * <li>先に送信された未コミット状態のメッセージがアボートされるとコミット済みメッセージがポールされること</li>
     * </ul>
     * </p>
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test004() throws InterruptedException, ExecutionException {
        // init Another Producer
        KafkaProducer<Integer, String> anotherProducer = new KafkaProducer<>(createProducerProps(),
                new IntegerSerializer(), new StringSerializer());
        anotherProducer.initTransactions();

        ProducerRecord<Integer, String> message1 = new ProducerRecord<>("topic1", 1, "test1");
        ProducerRecord<Integer, String> message2 = new ProducerRecord<>("topic1", 2, "test2");

        // start Transaction
        producer.beginTransaction();
        anotherProducer.beginTransaction();

        // anotherProducerでメッセージ送信
        anotherProducer.send(message1).get();

        // anotherProducer未コミット状態でポーリング
        ConsumerRecords<Integer, String> records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // producerでメッセージ送信
        producer.send(message2).get();

        // anotherProducer/producer未コミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // anotherProducerアボート
        anotherProducer.abortTransaction();

        // anotherProducerアボート状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(true));

        // producerコミット
        producer.commitTransaction();

        // producerコミット状態でポーリング
        records = consumer.poll(1000L);
        assertThat(records.isEmpty(), is(false));
        for (ConsumerRecord<Integer, String> record : records) {
            assertThat(record, hasPartition(0));
            assertThat(record, hasKey(2));
            assertThat(record, hasValue("test2"));
            System.out.println("polled message: [key] " + record.key() + ", [value] " + record.value());
        }

        // close anotherProducer
        anotherProducer.close();
    }

}
