//package com.xzc.flume.test.server; 
//
//import java.util.Properties;
//
//import kafka.javaapi.producer.Producer;
//import kafka.log.Log;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//
//import org.apache.zookeeper.Transaction;
//
//import scala.concurrent.Channel;
//import ch.qos.logback.core.Context;
//import ch.qos.logback.core.status.Status;
//
//import com.mysql.jdbc.log.LogFactory;
//import com.sun.jdi.event.Event;
//
///** 
// * @Desc  类描叙
// * @Author feelingxu@tcl.com: 
// * @Date 创建时间：2016年6月27日 下午1:55:49 
// * @Version V1.0.0
// */
//public class KafkaSink extends AbstractSinkimplementsConfigurable {  
//    
//    private static final Log logger = LogFactory.getLog(KafkaSink.class);  
//     
//    private Stringtopic;  
//    private Producer<String, String>producer;  
//    @Override  
//    public Status process()throwsEventDeliveryException {  
//          
//          Channel channel =getChannel();  
//       Transaction tx =channel.getTransaction();  
//       try {  
//               tx.begin();  
//               Event e = channel.take();  
//               if(e ==null) {  
//                       tx.rollback();  
//                       return Status.BACKOFF;  
//               }  
//               KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,newString(e.getBody()));  
//               producer.send(data);  
//               logger.info("Message: {}"+new String( e.getBody()));  
//               tx.commit();  
//               return Status.READY;  
//       } catch(Exceptione) {  
//         logger.error("KafkaSinkException:{}",e);  
//               tx.rollback();  
//               return Status.BACKOFF;  
//       } finally {  
//               tx.close();  
//       }  
//    }  
// 
//    @Override  
//    public void configure(Context context) {  
//         topic = "kafka";  
//          Properties props = newProperties();  
//              props.setProperty("metadata.broker.list","xx.xx.xx.xx:9092");  
//           props.setProperty("serializer.class","kafka.serializer.StringEncoder");  
////         props.setProperty("producer.type", "async");  
////         props.setProperty("batch.num.messages", "1");  
//            props.put("request.required.acks","1");  
//            ProducerConfigconfig = new ProducerConfig(props);  
//            producer = newProducer<String, String>(config);  
//    }  
//}  