package org.keith.demo.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by wubin
 */
public class ConsumerTest02 {

    public static void main(String[] args) {

        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("ProducerGroupName");
        pushConsumer.setNamesrvAddr("192.168.75.128:9876");
        pushConsumer.setInstanceName("Consumer");
        /**
         * 订阅指定topic下所有消息<br>
         * 注意：一个consumer对象可以订阅多个topic
         */
        try {
            pushConsumer.subscribe("TopicTest2", "*");
            pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
                 public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                	 	System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs.size());
                        MessageExt messageExt = msgs.get(0);
                        if("TopicTest2".equals(messageExt.getTopic())){
                            System.out.println(new String(messageExt.getBody()));
                        }
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                 }
            }
            );
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        try {
            pushConsumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
