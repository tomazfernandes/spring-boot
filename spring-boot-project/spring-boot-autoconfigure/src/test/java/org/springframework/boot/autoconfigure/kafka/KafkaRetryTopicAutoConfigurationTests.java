/*
 * Copyright 2012-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.autoconfigure.kafka;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link KafkaRetryTopicAutoConfiguration}.
 *
 * @author Tomaz Fernandes
 */
class KafkaRetryTopicAutoConfigurationTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));

	@Test
	void testGlobalDefaultConfiguration() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true").run((context) -> {
			RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
			assertThat(configuration.hasConfigurationForTopics(new String[] { "topic1", "topic2" })).isTrue();
		});
	}

	@Test
	void testIncludeTopics() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.include-topics[0]=topic1",
				"spring.kafka.retry-topic.testTopic.include-topics[1]=topic2").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					assertThat(configuration.hasConfigurationForTopics(new String[] { "topic1", "topic2" })).isTrue();
				});
	}

	@Test
	void testMultipleConfigurations() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.first-test-topic.include-topics=topic1",
				"spring.kafka.retry-topic.second-test-topic.include-topics=topic2").run((context) -> {
					Map<String, RetryTopicConfiguration> configurations = context
							.getBeansOfType(RetryTopicConfiguration.class);
					assertThat(configurations.size()).isEqualTo(2);
					assertThat(
							configurations.get("first-test-topic").hasConfigurationForTopics(new String[] { "topic1" }))
									.isTrue();
					assertThat(
							configurations.get("first-test-topic").hasConfigurationForTopics(new String[] { "topic2" }))
									.isFalse();
					assertThat(configurations.get("second-test-topic")
							.hasConfigurationForTopics(new String[] { "topic1" })).isFalse();
					assertThat(configurations.get("second-test-topic")
							.hasConfigurationForTopics(new String[] { "topic2" })).isTrue();
				});
	}

	@Test
	void testExcludeTopics() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.exclude-topics[0]=topic1",
				"spring.kafka.retry-topic.testTopic.exclude-topics[1]=topic2").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					assertThat(configuration.hasConfigurationForTopics(new String[] { "topic1", "topic2" })).isFalse();
				});
	}

	@Test
	void testAttemptsAndExponentialBackOff() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.attempts=4",
				"spring.kafka.retry-topic.testTopic.backOff.delay=500",
				"spring.kafka.retry-topic.testTopic.backOff.maxDelay=1500",
				"spring.kafka.retry-topic.testTopic.backOff.multiplier=2").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(5);
					assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(500);
					assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(1000);
					assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(1500);
					assertThat(destinationTopicProperties.get(4).delay()).isEqualTo(0);
				});
	}

	@Test
	void testExponentialBackOffWithDefaults() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.backOff.multiplier=2").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(4);
					assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(1000);
					assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(2000);
					assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);
				});
	}

	@Test
	void testFixedBackOff() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.backOff.delay=1s",
				"spring.kafka.retry-topic.testTopic.backOff.delay=1s"
				).run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(4);
					assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(1000);
					assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(1000);
					assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);
				});
	}

	@Test
	void testNoBackOff() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.backOff.delay=0").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(4);
					assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(0);
					assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);
				});
	}

	@Test
	void testRetryOn() {
		this.contextRunner
				.withPropertyValues("spring.kafka.retry-topic-enabled=true",
						"spring.kafka.retry-topic.testTopic.retry-on[0]=java.lang.IllegalArgumentException",
						"spring.kafka.retry-topic.testTopic.retry-on[1]=java.lang.IllegalStateException")
				.run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					DestinationTopic topic = new DestinationTopic("testTopic", destinationTopicProperties.get(0));
					assertThat(topic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
					assertThat(topic.shouldRetryOn(0, new IllegalStateException())).isTrue();
					assertThat(topic.shouldRetryOn(0, new IllegalAccessException())).isFalse();
				});
	}

	@Test
	void testTraversingCauses() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.retry-on[0]=java.lang.IllegalArgumentException",
				"spring.kafka.retry-topic.testTopic.traversing-causes=true").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					DestinationTopic topic = new DestinationTopic("testTopic", destinationTopicProperties.get(0));
					assertThat(
							topic.shouldRetryOn(0, new IllegalStateException("test", new IllegalArgumentException())))
									.isTrue();
				});
	}

	@Test
	void testNotRetryOn() {
		this.contextRunner
				.withPropertyValues("spring.kafka.retry-topic-enabled=true",
						"spring.kafka.retry-topic.testTopic.not-retry-on[0]=java.lang.IllegalArgumentException",
						"spring.kafka.retry-topic.testTopic.not-retry-on[1]=java.lang.IllegalStateException")
				.run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					DestinationTopic topic = new DestinationTopic("testTopic", destinationTopicProperties.get(0));
					assertThat(topic.shouldRetryOn(0, new IllegalArgumentException())).isFalse();
					assertThat(topic.shouldRetryOn(0, new IllegalStateException())).isFalse();
					assertThat(topic.shouldRetryOn(0, new IllegalAccessException())).isTrue();
				});
	}

	@Test
	void testTimeout() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.timeout=10s").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					DestinationTopic topic = new DestinationTopic("testTopic", destinationTopicProperties.get(0));
					assertThat(topic.getDestinationTimeout()).isEqualTo(10000);
				});
	}

	@Test
	void testKafkaTemplate() {
		KafkaOperations<Object, Object> kafkaTemplate = mock(KafkaOperations.class);
		this.contextRunner
				.withPropertyValues("spring.kafka.retry-topic-enabled=true",
						"spring.kafka.retry-topic.testTopic.kafka-template=myKafkaTemplate")
				.withBean("myKafkaTemplate", KafkaOperations.class, () -> kafkaTemplate).run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					DestinationTopic topic = new DestinationTopic("testTopic", destinationTopicProperties.get(0));
					assertThat(topic.getKafkaOperations()).isEqualTo(kafkaTemplate);
				});
	}

	@Test
	void testListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = mock(
				ConcurrentKafkaListenerContainerFactory.class);
		this.contextRunner
				.withPropertyValues("spring.kafka.retry-topic-enabled=true",
						"spring.kafka.retry-topic.testTopic.listener-container-factory=myListenerContainerFactory")
				.withBean("myListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class, () -> factory)
				.run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					assertThat(configuration.forContainerFactoryResolver())
							.hasFieldOrPropertyWithValue("listenerContainerFactoryName", "myListenerContainerFactory");
				});
	}

	@Test
	void testSuffixes() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.retry-topic-suffix=test.suffix",
				"spring.kafka.retry-topic.testTopic.dlt-suffix=dlt.suffix").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					assertThat(destinationTopicProperties.get(0).suffix()).isEmpty();
					assertThat(destinationTopicProperties.get(1).suffix()).isEqualTo("test.suffix-0");
					assertThat(destinationTopicProperties.get(2).suffix()).isEqualTo("test.suffix-1");
					assertThat(destinationTopicProperties.get(3).suffix()).isEqualTo("dlt.suffix");
				});
	}

	@Test
	void testDltHandler() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.dlt-handler-class=org.springframework.boot.autoconfigure.kafka.KafkaRetryTopicAutoConfigurationTests.MyTestDltHandler",
				"spring.kafka.retry-topic.testTopic.dlt-handler-method=listenDlt").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					assertThat(configuration.getDltHandlerMethod().getMethod().getName()).isEqualTo("listenDlt");
					assertThat(configuration.getDltHandlerMethod().resolveBean(context.getBeanFactory()))
							.isInstanceOf(MyTestDltHandler.class);
				});
	}

	@Test
	void testFixedDelayStrategy() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.fixed-delay-strategy=SINGLE_TOPIC").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties).isNotEmpty();
					assertThat(destinationTopicProperties.get(0).suffix()).isEmpty();
					assertThat(destinationTopicProperties.get(1).suffix()).isEqualTo("-retry");
					assertThat(destinationTopicProperties.get(2).suffix()).isEqualTo("-dlt");
				});
	}

	@Test
	void testDltStrategy() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.dlt-strategy=NO_DLT").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(3);
					assertThat(destinationTopicProperties.get(0).suffix()).isEmpty();
					assertThat(destinationTopicProperties.get(1).suffix()).isEqualTo("-retry-0");
					assertThat(destinationTopicProperties.get(2).suffix()).isEqualTo("-retry-1");
				});
	}

	@Test
	void testTopicSuffixingStrategy() {
		this.contextRunner
				.withPropertyValues("spring.kafka.retry-topic-enabled=true",
						"spring.kafka.retry-topic.testTopic.back-off.delay=1s",
						"spring.kafka.retry-topic.testTopic.back-off.multiplier=2",
						"spring.kafka.retry-topic.testTopic.topic-suffixing-strategy=SUFFIX_WITH_INDEX_VALUE")
				.run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					List<DestinationTopic.Properties> destinationTopicProperties = configuration
							.getDestinationTopicProperties();
					assertThat(destinationTopicProperties.size()).isEqualTo(4);
					assertThat(destinationTopicProperties.get(0).suffix()).isEmpty();
					assertThat(destinationTopicProperties.get(1).suffix()).isEqualTo("-retry-0");
					assertThat(destinationTopicProperties.get(2).suffix()).isEqualTo("-retry-1");
				});
	}

	@Test
	void testTopicAutoCreation() {
		this.contextRunner.withPropertyValues("spring.kafka.retry-topic-enabled=true",
				"spring.kafka.retry-topic.testTopic.auto-create-topics.enabled=false",
				"spring.kafka.retry-topic.testTopic.auto-create-topics.number-of-partitions=5",
				"spring.kafka.retry-topic.testTopic.auto-create-topics.replication-factor=5").run((context) -> {
					RetryTopicConfiguration configuration = context.getBean(RetryTopicConfiguration.class);
					assertThat(configuration.forKafkaTopicAutoCreation())
							.hasFieldOrPropertyWithValue("shouldCreateTopics", false);
					assertThat(configuration.forKafkaTopicAutoCreation()).hasFieldOrPropertyWithValue("numPartitions",
							5);
					assertThat(configuration.forKafkaTopicAutoCreation())
							.hasFieldOrPropertyWithValue("replicationFactor", (short) 5);
				});
	}

	public static class MyTestDltHandler {

		void listenDlt() {
		}

	}

}
