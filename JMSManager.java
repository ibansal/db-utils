package com.bsb.portal.db;

import java.util.HashMap;
import java.util.Map;

import javax.jms.*;
import javax.jms.IllegalStateException;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.client.HornetQQueue;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

import com.bsb.portal.config.JMSConfig;

public class JMSManager implements ExceptionListener {

    private static final Logger logger = LoggerFactory.getLogger(JMSManager.class.getCanonicalName());

    private JMSConfig           config;

    private MessageProducer     producer;
    private MessageConsumer     consumer;
    private Session             session;

    private Connection          queueConnection;

    public JMSManager(JMSConfig config) {
        this.config = config;
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
            if(Session.CLIENT_ACKNOWLEDGE == config.getAcknowledgementMode()) {
                session = queueConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            }
            else {
                session = queueConnection.createSession(false, config.getAcknowledgementMode());
            }
            if(config.isConsumer()) {
                consumer = session.createConsumer(new HornetQQueue(config.getQueueName()));
            }
            if(config.isProducer()) {
                producer = session.createProducer(new HornetQQueue(config.getQueueName()));
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            }

        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void sendMessage(JSONObject jsonObj) {
        try {
            ObjectMessage textMessage = session.createObjectMessage(jsonObj);
            producer.send(textMessage);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void sendMessage(String strMessage) throws JMSException {
        if(strMessage == null)
            return;
        ObjectMessage textMessage = session.createObjectMessage(strMessage);
        producer.send(textMessage);
    }

    public void setMessageListener(MessageListener messageListener) {
        if(consumer != null) {
            try {
                consumer.setMessageListener(messageListener);
                //connection should be started after message listener is set, else there will be message loss.
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
    
    public void commit() {
        try {
            session.commit();
        }
        catch (JMSException ex) {
            logger.error("Exception in calling commit : " + ex.getMessage(), ex);
        }
    }

    public void rollBack() {
        try {
            session.rollback();
        }
        catch (JMSException ex) {
            logger.error("Exception in calling rollBack : " + ex.getMessage(), ex);
        }
    }

    public static void main(String[] args) {
        try {
            JMSConfig config = new JMSConfig();
            config.setHost("ec2-54-227-82-219.compute-1.amazonaws.com"); //hornetq
            config.setPort(5445);
            config.setJMSTurnON(true);
            config.setConsumer(true);
            config.setProducer(true);
            config.setQueueName("gcmMessageQueue");
            config.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);

            JMSManager jmsManager = new JMSManager(config);
            jmsManager.init();
            for (int i = 0; i < 10; i++) {
                jmsManager.sendMessage("test : "+i);

            }

            jmsManager.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    System.out.println("Received : " + message);
                }
            });

            while(true)
                System.in.read();


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
