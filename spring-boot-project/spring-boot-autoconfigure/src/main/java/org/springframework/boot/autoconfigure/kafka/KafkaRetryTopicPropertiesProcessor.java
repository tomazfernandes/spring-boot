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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.NoBackOffPolicy;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.util.ClassUtils;

/**
 *
 * Processes a {@link KafkaProperties.RetryTopicProperties} instance,
 * returning a {@link RetryTopicConfiguration} object.
 *
 * @author Tomaz Fernandes
 * @since 2.6.0
 */
public class KafkaRetryTopicPropertiesProcessor {

	private static final int DEFAULT_BACKOFF_ANNOTATION_VALUE = 1000;
	private final BeanFactory beanFactory;

	KafkaRetryTopicPropertiesProcessor(BeanFactory beanFactory){
		this.beanFactory = beanFactory;
	}

	/**
	 * Processes the given properties.
	 *
	 * @param retryTopicProperties the properties to be processed.
	 * @param configurationName the name of the provided configuration.
	 * @return the {@link RetryTopicConfiguration}.
	 */
	public RetryTopicConfiguration process(KafkaProperties.RetryTopicProperties retryTopicProperties,
										String configurationName) {

		RetryTopicConfigurationBuilder builder = RetryTopicConfigurationBuilder.newInstance();

		JavaUtils.INSTANCE.acceptIfNotNull(retryTopicProperties.getAttempts(), builder::maxAttempts)
				.acceptIfNotNull(retryTopicProperties.getBackOffIfNotEmpty(),
						(backOff) -> addBackOffPolicy(builder, backOff))
				.acceptIfNotNull(retryTopicProperties.getRetryTopicSuffix(), builder::retryTopicSuffix)
				.acceptIfNotNull(retryTopicProperties.getDltSuffix(), builder::dltSuffix)
				.acceptIfNotNull(retryTopicProperties.getListenerContainerFactory(), builder::listenerFactory)
				.acceptIfNotNull(retryTopicProperties.getIncludeTopics(), builder::includeTopics)
				.acceptIfNotNull(retryTopicProperties.getExcludeTopics(), builder::excludeTopics)
				.acceptIfNotNull(retryTopicProperties.getDltHandlerClass(), retryTopicProperties.getDltHandlerMethod(),
						(className, methodName) -> builder.dltHandlerMethod(resolveDltHandlerClass(className), methodName))
				.acceptIfNotNull(retryTopicProperties.getAutoCreateTopicsIfNotEmpty(),
						(config) -> builder.autoCreateTopics(config.isEnabled(), config.getNumberOfPartitions(),
								config.getReplicationFactor()))
				.acceptIfNotNull(retryTopicProperties.getRetryOn(),
						(include) -> builder.retryOn(resolveThrowableClasses(include, "retryOn")))
				.acceptIfNotNull(retryTopicProperties.getNotRetryOn(),
						(exclude) -> builder.notRetryOn(resolveThrowableClasses(exclude, "notRetryOn")))
				.acceptIfNotNull(retryTopicProperties.getDltSuffix(), builder::dltSuffix)
				.acceptIfNotNull(retryTopicProperties.isTraversingCauses(), builder::traversingCauses)
				.acceptIfNotNull(retryTopicProperties.getTimeoutMillisIfPresent(), builder::timeoutAfter)
				.acceptIfNotNull(retryTopicProperties.getTopicSuffixingStrategy(), builder::setTopicSuffixingStrategy)
				.acceptIfNotNull(retryTopicProperties.getDltStrategy(), builder::dltProcessingFailureStrategy)
				.acceptIfNotNull(retryTopicProperties.getFixedDelayStrategy(), builder::useSingleTopicForFixedDelays);

		return builder.create(getKafkaOperationsBean(retryTopicProperties, configurationName));
	}

	private KafkaOperations<?, ?> getKafkaOperationsBean(KafkaProperties.RetryTopicProperties retryTopicProperties,
														String configurationName) {
		String kafkaTemplateBeanName = retryTopicProperties.getKafkaTemplate();
		try {
			return this.beanFactory.getBean(kafkaTemplateBeanName, KafkaOperations.class);
		} catch (NoSuchBeanDefinitionException ex) {
			throw new IllegalStateException("No " + KafkaOperations.class.getSimpleName()
					+ " bean with name" + kafkaTemplateBeanName + " found while processing configuration "
					+ configurationName);
		} catch (BeanNotOfRequiredTypeException ex) {
			throw new IllegalStateException("Bean with name" + kafkaTemplateBeanName +
					" not an instance of " + KafkaOperations.class.getSimpleName()
					+ " while processing configuration " + configurationName);
		}
	}

	private void addBackOffPolicy(RetryTopicConfigurationBuilder builder,
								  KafkaProperties.RetryTopicProperties.BackOff backOff) {
		BackOffPolicy backOffPolicy = createBackOff(backOff);
		if (SleepingBackOffPolicy.class.isAssignableFrom(backOffPolicy.getClass())) {
			builder.customBackoff((SleepingBackOffPolicy<?>) backOffPolicy);
		} else {
			builder.noBackoff();
		}
	}

	private Class<?> resolveDltHandlerClass(String className) {
		try {
			return ClassUtils.forName(className, ClassUtils.getDefaultClassLoader());
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalArgumentException("No class found for name " + className, ex);
		}
	}

	@SuppressWarnings("unchecked")
	private List<Class<? extends Throwable>> resolveThrowableClasses(List<String> names, String type) {

		List<Class<? extends Throwable>> classes = new ArrayList<>(names.size());
		try {
			for (String name : names) {
				Class<?> clazz = ClassUtils.forName(name, ClassUtils.getDefaultClassLoader());
				if (!Throwable.class.isAssignableFrom(clazz)) {
					throw new IllegalStateException(type + " entry must be of type Throwable: " + clazz);
				}
				classes.add((Class<? extends Throwable>) clazz);
			}
		}
		catch (ClassNotFoundException | LinkageError ex) {
			throw new IllegalStateException(ex);
		}
		return classes;
	}

	private BackOffPolicy createBackOff(KafkaProperties.RetryTopicProperties.BackOff backOff) {
		long min = (backOff.getDelay() != null) ? backOff.getDelay().toMillis() : DEFAULT_BACKOFF_ANNOTATION_VALUE;
		long max = (backOff.getMaxDelay() != null) ? backOff.getMaxDelay().toMillis() : 0;
		double multiplier = (backOff.getMultiplier() != null) ? backOff.getMultiplier() : 0;
		boolean isRandom = backOff.isRandom() != null && backOff.isRandom();
		return doCreateBackOff(min, max, multiplier, isRandom);
	}

	private BackOffPolicy doCreateBackOff(long min, long max, double multiplier, boolean isRandom) {
		if (multiplier > 0) {
			ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
			if (isRandom) {
				policy = new ExponentialRandomBackOffPolicy();
			}
			policy.setInitialInterval(min);
			policy.setMultiplier(multiplier);
			policy.setMaxInterval((max > min) ? max : ExponentialBackOffPolicy.DEFAULT_MAX_INTERVAL);
			return policy;
		}
		if (max > min) {
			UniformRandomBackOffPolicy policy = new UniformRandomBackOffPolicy();
			policy.setMinBackOffPeriod(min);
			policy.setMaxBackOffPeriod(max);
			return policy;
		}

		if (min == 0) {
			return new NoBackOffPolicy();
		}

		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		policy.setBackOffPeriod(min);
		return policy;
	}
}
