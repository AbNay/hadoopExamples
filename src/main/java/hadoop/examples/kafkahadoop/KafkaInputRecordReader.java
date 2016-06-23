package hadoop.examples.kafkahadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputRecordReader extends RecordReader<LongWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private Configuration conf;

    private KafkaInputSplit split;
    //private TaskAttemptContext context;

    private SimpleConsumer consumer ;
    private int fetchSize;
    private String topic;
    private String reset;

    private int partition;
    private long earliestOffset;
    private long watermark;
    private long latestOffset;

    private FetchResponse messages;
    private Iterator<MessageAndOffset> iterator;
    private LongWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException
    {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        topic = this.split.getTopic();
        partition = this.split.getPartition();
        watermark = this.split.getWatermark();

        int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
        int bufferSize = conf.getInt("kafka.socket.buffersize", 64*1024);
        consumer =  new SimpleConsumer(this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize,"{xxx}");

        fetchSize = conf.getInt("kafka.fetch.size", 1024 * 1024);
        reset = conf.get("kafka.watermark.reset", "watermark");
        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        //log.info("Last watermark for {} to {}", topic +":"+partition, watermark);

        if ("earliest".equals(reset)) {
            resetWatermark(-1);
        } else if("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (watermark < earliestOffset) {
            resetWatermark(-1);
        }

        log.info(
            "Split {} Topic: {} Broker: {} Partition: {} Earliest: {} Latest: {} Starting: {}", 
            new Object[]{this.split, topic, this.split.getBrokerId(), partition, earliestOffset, latestOffset, watermark }
        );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }

        if (messages == null) {
        	FetchRequest request = new FetchRequestBuilder().clientId("{xxx}").addFetch(topic, partition, watermark, fetchSize)
                    .maxWait(conf.getInt("kafka.socket.timeout.ms", 30000)).build();
            //FetchRequest request = new FetchRequest(topic, partition, watermark, fetchSize);
            log.info("{} fetching offset {} ", topic+":" + split.getBrokerId() +":" + partition, watermark);
            messages = consumer.fetch(request);
            if (messages.errorCode(topic, partition)//getErrorCode()
            		== ErrorMapping.OffsetOutOfRangeCode())
            {
                log.info("Out of bounds = " + watermark);
                return false;
            }
            if (messages.errorCode(topic, partition)//getErrorCode()
            		!= 0)
            {
                log.warn("Messages fetch error code: " + messages.errorCode(topic, partition)/*getErrorCode()*/);
                return false;
            } else {
                iterator = messages.messageSet(topic, partition).iterator();
                watermark += messages.messageSet(topic, partition).validBytes();
                if (!iterator.hasNext())
                {
                    //log.info("No more messages");
                    return false;
                }
            }
        }

        if (iterator.hasNext())
        {
            MessageAndOffset messageOffset = iterator.next();
            Message message = messageOffset.message();
            key.set(watermark - message.size() - 4);
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            numProcessedMessages++;
            if (!iterator.hasNext())
            {
                messages = null;
                iterator = null;
            }
            return true;
        }
        log.warn("Unexpected iterator end.");
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException 
    {
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float)(latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException
    {
        log.info("{} num. processed messages {} ", topic+":" + split.getBrokerId() +":" + partition, numProcessedMessages);
        if (numProcessedMessages >0)
        {
            ZkUtils zk = new ZkUtils(
                conf.get("kafka.zk.connect"),
                conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
                conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
            );

            String group = conf.get("kafka.groupid");
            String partition = split.getBrokerId() + "-" + split.getPartition();
            zk.commitLastConsumedOffset(group, split.getTopic(), partition, watermark);
            zk.close();
        }
        consumer.close();
    }

    private long getLastOffset(long whichTime, String clientName) {
    	TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
    			partition);
    	Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    	requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
    			whichTime, 1));
    	kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
    			requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
    			clientName);
    	OffsetResponse response = consumer.getOffsetsBefore(request);

    	if (response.hasError()) {
    		return 0;
    	}
    	long[] offsets = response.offsets(topic, partition);
    	return offsets[0];
    }
    
    private long getEarliestOffset() {
        if (earliestOffset <= 0) {        	 
            //earliestOffset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
        	earliestOffset = getLastOffset(-2L, "{xxx}");
        }
        return earliestOffset;
    }

    private long getLatestOffset() {
        if (latestOffset <= 0) {
            //latestOffset = consumer.getOffsetsBefore(topic, partition, -1L, 1)[0];
            earliestOffset = getLastOffset(-1L, "{xxx}");
        }
        return latestOffset;
    }

    private void resetWatermark(long offset) {
        if (offset <= 0) {
            //offset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
            earliestOffset = getLastOffset(-2L, "{xxx}");
        }
        log.info("{} resetting offset to {}", topic+":" + split.getBrokerId() +":" + partition, offset);
        watermark = earliestOffset = offset;
    }

}
