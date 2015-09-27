package com.bsb.portal.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.client.HornetQQueue;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.bsb.portal.common.ProductIdQueueMapping;
import com.bsb.portal.common.Triple;
import com.bsb.portal.config.JMSConfig;
import com.bsb.portal.util.HttpClient;
import com.google.gson.Gson;

public class ConfigurableJMSManager implements ExceptionListener {

    private static final Logger                                             logger                    = LoggerFactory.getLogger(ConfigurableJMSManager.class.getCanonicalName());
    private static final Gson                                               gson                      = new Gson();
    private static final int                                                TEN_SEC                   = 10000;
    private JMSConfig                                                       config;

    private Session                                                         session;
    private MessageConsumer                                                 consumer;
    private MessageProducer                                                 producer;
    private Connection                                                      queueConnection;

    private final ReadWriteLock                                             lock                      = new ReentrantReadWriteLock();
    private final Lock                                                      readLock                  = lock.readLock();
    private final Lock                                                      writeLock                 = lock.writeLock();

    private Map<String, Set<Integer>>                                       endpointToProductIdCache  = new ConcurrentHashMap<>();
    private Map<Integer, Triple<Session, MessageConsumer, MessageProducer>> productIdToJMSTripleCache = new ConcurrentHashMap<>();
    private Map<String, Triple<Session, MessageConsumer, MessageProducer>>  queueToJMSTripleCache     = new ConcurrentHashMap<>();

    public ConfigurableJMSManager(JMSConfig config) {
        this.config = config;
    }

    @Scheduled(fixedDelay = 1000 * 60 * 60)
    public void buildCache() {
        Set<String> endpoints = config.getEndpoints();
        if(null == endpoints || endpoints.isEmpty()) {
            logger.warn("No endpoints defined, I will use the default queue");
            return;
        }

        try {
            writeLock.lock();
            for(String endpoint : endpoints) {
                ProductIdQueueMapping productIdQueueMapping = fetchProductIdToQueueMapping(endpoint);
                if(null == productIdQueueMapping) {
                    logger.warn("No mapping found for endpoint: {}", endpoint);
                    continue;
                }
                buildEndpointToProductIdCache(endpoint, productIdQueueMapping);
                String queue = productIdQueueMapping.getQueueName();
                Triple<Session, MessageConsumer, MessageProducer> jmsTriple = queueToJMSTripleCache.get(queue);
                Set<Integer> productIds = productIdQueueMapping.getProductIds();
                if(null == jmsTriple) {
                    try {
                        logger.debug("No JMSTriple found in the cache for queue: {}, creating it", queue);
                        jmsTriple = createJmsTripleForQueue(queue);
                        queueToJMSTripleCache.put(queue, jmsTriple);
                    }
                    catch (JMSException e) {
                        logger.error("Error creating JMS Triple, I will use the default queue for productId: {}, Error: {}", productIds, e.getMessage(), e);
                        if(null == jmsTriple) {
                            jmsTriple = new Triple<Session, MessageConsumer, MessageProducer>(session, consumer, producer);
                        }
                    }
                }
                for(Integer productId : productIds) {
                    productIdToJMSTripleCache.put(productId, jmsTriple);
                }
            }
        }
        finally {
            writeLock.unlock();
        }

    }

    private void buildEndpointToProductIdCache(String endpoint, ProductIdQueueMapping productIdQueueMapping) {
        // clear ProductId to Queue Mapping
        Set<Integer> productIdSet = endpointToProductIdCache.get(endpoint);
        if(null != productIdSet) {
            logger.debug("Clearing existing endpointToProductIdCache: {} for endpoint: {}", productIdSet, endpoint);
            for(Integer productId : productIdSet) {
                productIdToJMSTripleCache.remove(productId);
            }
        }

        // build endpointToProductIdCache
        endpointToProductIdCache.put(endpoint, productIdQueueMapping.getProductIds());

    }

    private ProductIdQueueMapping fetchProductIdToQueueMapping(String endPoint) {
        ProductIdQueueMapping mapping = null;
        String responseJson;
        try {
            responseJson = HttpClient.getContent(endPoint, TEN_SEC, new HashMap<String, String>());
            logger.info("Endpoint: {}, Response: {}", endPoint, responseJson);
            mapping = gson.fromJson(responseJson, ProductIdQueueMapping.class);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return mapping;
    }

    public void init() {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("host", config.getHost());
        params.put("port", config.getPort());
        TransportConfiguration transportConfig = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
        HornetQJMSConnectionFactory connectionFactory = new HornetQJMSConnectionFactory(false, transportConfig);
        connectionFactory.setRetryInterval(5000);
        connectionFactory.setReconnectAttempts(10000);
        connectionFactory.setRetryIntervalMultiplier(1.2);
        connectionFactory.setMaxRetryInterval(60000);
        CachingConnectionFactory cachedConnectionFactory = new CachingConnectionFactory(connectionFactory);
        cachedConnectionFactory.setReconnectOnException(true);
        cachedConnectionFactory.setExceptionListener(this);
        try {
            queueConnection = cachedConnectionFactory.createQueueConnection();
            Triple<Session, MessageConsumer, MessageProducer> jmsTriple = createJmsTripleForQueue(config.getDefaultQueueName());
            session = jmsTriple.getLeft();
            consumer = jmsTriple.getMiddle();
            producer = jmsTriple.getRight();
            buildCache();
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Triple<Session, MessageConsumer, MessageProducer> createJmsTripleForQueue(String queueName) throws JMSException {
        Session session = null;
        MessageConsumer consumer = null;
        MessageProducer producer = null;
        if(Session.CLIENT_ACKNOWLEDGE == config.getAcknowledgementMode()) {
            session = queueConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        }
        else {
            session = queueConnection.createSession(false, config.getAcknowledgementMode());
        }
        if(config.isConsumer()) {
            consumer = session.createConsumer(new HornetQQueue(queueName));
        }
        if(config.isProducer()) {
            producer = session.createProducer(new HornetQQueue(queueName));
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        }
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = new Triple<>(session, consumer, producer);

        return jmsTriple;
    }

    private Triple<Session, MessageConsumer, MessageProducer> getJMSTripleForProductId(Integer productId) {
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = null;
        readLock.lock();
        try {
            if(null != productId)
                jmsTriple = productIdToJMSTripleCache.get(productId);

            if(null == jmsTriple)
                jmsTriple = new Triple<>(session, consumer, producer);
        }
        finally {
            readLock.unlock();
        }
        return jmsTriple;
    }

    private Triple<Session, MessageConsumer, MessageProducer> getJMSTripleForQueue(String queueName) {
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = null;
        readLock.lock();
        try {
            if(StringUtils.isNotBlank(queueName))
                jmsTriple = queueToJMSTripleCache.get(queueName);

            if(null == jmsTriple)
                jmsTriple = new Triple<>(session, consumer, producer);
        }
        finally {
            readLock.unlock();
        }
        return jmsTriple;
    }

    public void sendMessage(JSONObject jsonObj, Integer productId) {
        try {
            Triple<Session, MessageConsumer, MessageProducer> jmsTriple = getJMSTripleForProductId(productId);
            ObjectMessage textMessage = jmsTriple.getLeft().createObjectMessage(jsonObj);
            jmsTriple.getRight().send(textMessage);
        }
        catch (Exception e) {
            logger.error("Exception while sending JMS message", e.getMessage(), e);
        }
    }

    public void sendMessage(String strMessage, Integer productId) throws JMSException {
        if(strMessage == null)
            return;
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = getJMSTripleForProductId(productId);
        ObjectMessage textMessage = jmsTriple.getLeft().createObjectMessage(strMessage);
        jmsTriple.getRight().send(textMessage);
    }

    public void setMessageListener(MessageListener messageListener, String queueName) {
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = getJMSTripleForQueue(queueName);
        if(jmsTriple.getMiddle() != null) {
            try {
                jmsTriple.getMiddle().setMessageListener(messageListener);
                // connection should be started after message listener is set, else there will be
                // message loss.
                queueConnection.start();
            }
            catch (JMSException e) {
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void onException(JMSException e) {
        logger.error(e.getMessage(), e);
        if(e instanceof IllegalStateException || e instanceof InvalidClientIDException || e instanceof InvalidDestinationException || e instanceof InvalidSelectorException) {
            init();
        }
    }

    public boolean isJMSOn() {
        if(config != null) {
            return config.isJMSTurnON();
        }
        return false;
    }

    public void commit(String queueName) {
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = getJMSTripleForQueue(queueName);
        try {
            jmsTriple.getLeft().commit();
        }
        catch (JMSException ex) {
            logger.error("Exception in calling commit: {}", ex.getMessage(), ex);
        }
    }

    public void rollBack(String queueName) {
        Triple<Session, MessageConsumer, MessageProducer> jmsTriple = getJMSTripleForQueue(queueName);
        try {
            jmsTriple.getLeft().rollback();
        }
        catch (JMSException ex) {
            logger.error("Exception in calling rollBack: {}", ex.getMessage(), ex);
        }
    }

}
