package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    
    private final UserRepository userRepository;
    
    public KafkaConsumer(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void consume(Transaction transaction) {
        logger.info("Processing transaction: {}", transaction);
        
        // Get sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        
        if (sender != null && recipient != null) {
            // Update balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());
            
            // Save to database
            userRepository.save(sender);
            userRepository.save(recipient);
            
            logger.info("Transaction processed: {} sent {} to {}", 
                sender.getName(), transaction.getAmount(), recipient.getName());
        } else {
            logger.error("Could not process transaction - user not found");
        }
    }
}