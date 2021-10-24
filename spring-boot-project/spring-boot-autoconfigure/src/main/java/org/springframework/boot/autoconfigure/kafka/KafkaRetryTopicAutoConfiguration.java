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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;

import java.util.Map;

/**
 * Autoconfiguration for Kafka non-blocking delayed retries using topics.
 *
 * @author Tomaz Fernandes
 * @since 2.6.0
 */
@Configuration(proxyBeanMethods = false)
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnClass(RetryableTopic.class)
@ConditionalOnProperty(KafkaRetryTopicAutoConfiguration.RETRY_TOPIC_ENABLED_PROPERTY)
public class KafkaRetryTopicAutoConfiguration {

	final static String RETRY_TOPIC_ENABLED_PROPERTY = "spring.kafka.retry-topic-enabled";

	private final KafkaProperties properties;

	private final KafkaRetryTopicPropertiesProcessor propertiesProcessor;

	private final ConfigurableBeanFactory beanRegistry;

	public KafkaRetryTopicAutoConfiguration(KafkaProperties properties,
											ConfigurableBeanFactory beanRegistry,
											KafkaRetryTopicPropertiesProcessor propertiesProcessor) {
		this.properties = properties;
		this.propertiesProcessor = propertiesProcessor;
		this.beanRegistry = beanRegistry;
		this.properties
				.getRetryTopicOrDefault()
				.forEach(this::registerConfigurationFromProperties);
	}

	private void registerConfigurationFromProperties(String configurationName,
													KafkaProperties.RetryTopicProperties retryTopicProperties) {

		this.beanRegistry.registerSingleton(configurationName,
				this.propertiesProcessor.process(retryTopicProperties, configurationName));
	}

	@Bean
	RetryTopicConfiguration retryTopicConfiguration() {
		// Workaround so that this class gets instantiated when RetryTopicConfiguration
		// beans are looked up in RetryTopic bootstrapping.
		// We probably should have an aggregation class instead.
		Map.Entry<String, KafkaProperties.RetryTopicProperties> propertiesEntry =
				this.properties.getRetryTopic().entrySet().stream().findFirst().orElseThrow(
						() -> new IllegalStateException("No configuration provided!"));
		return this.propertiesProcessor.process(propertiesEntry.getValue(), propertiesEntry.getKey());
	}

	@Bean(name = RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME)
	@ConditionalOnMissingBean(name = RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME)
	ConcurrentKafkaListenerContainerFactory<?, ?> retryTopicListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory
				.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
		return factory;
	}

	@Configuration
	@ConditionalOnProperty(RETRY_TOPIC_ENABLED_PROPERTY)
	@ConditionalOnClass(RetryableTopic.class)
	public static class PropertiesProcessor {

		@Bean
		@ConditionalOnMissingBean
		KafkaRetryTopicPropertiesProcessor propertiesProcessor(BeanFactory beanFactory) {
			return new KafkaRetryTopicPropertiesProcessor(beanFactory);
		}
	}
}
