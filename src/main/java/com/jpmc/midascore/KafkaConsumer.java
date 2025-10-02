package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;
    
    public KafkaConsumer(UserRepository userRepository, 
                        TransactionRepository transactionRepository,
                        IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }
    
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void consume(Transaction transaction) {
        logger.info("Processing transaction: {}", transaction);
        
        // Get sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        
        // Validate transaction
        if (sender == null || recipient == null) {
            logger.error("Transaction rejected - user not found");
            return;
        }
        
        if (sender.getBalance() < transaction.getAmount()) {
            logger.error("Transaction rejected - insufficient balance");
            return;
        }
        
        // Get incentive from API
        Incentive incentive = incentiveService.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();
        
        // Process valid transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        
        // Save users
        userRepository.save(sender);
        userRepository.save(recipient);
        
        // Save transaction record with incentive
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(record);
        
        logger.info("Transaction processed: {} sent {} to {} (incentive: {})", 
            sender.getName(), transaction.getAmount(), recipient.getName(), incentiveAmount);
    }
}