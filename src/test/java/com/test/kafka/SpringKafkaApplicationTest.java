package com.test.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import com.test.kafka.consumer.KafkaReceiver;
import com.test.kafka.producer.KafkaSender;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaApplicationTest {

  protected final static String TEST_TOPIC = "test.t";

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  private KafkaReceiver kafkaReceiver;

  @Autowired
  private KafkaSender kafkaSender;

  @ClassRule
  public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true, TEST_TOPIC);

  @Before
  public void runBeforeTestMethod() throws Exception {
    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
        .getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer,
          kafkaEmbedded.getPartitionsPerTopic());
    }
  }

  @Test
  public void testReceive() throws Exception {
    kafkaSender.send(TEST_TOPIC, "This is my frist Spring Kafka Demo");

    kafkaReceiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
    assertThat(kafkaReceiver.getLatch().getCount()).isEqualTo(0);
  }
}
