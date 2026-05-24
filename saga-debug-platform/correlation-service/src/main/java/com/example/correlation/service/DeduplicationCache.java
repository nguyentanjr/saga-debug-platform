package com.example.correlation.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;





@Component
public class DeduplicationCache {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationCache.class);

    private final Cache<String, Boolean> cache;

    public DeduplicationCache() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterWrite(Duration.ofHours(1))
                .build();
        log.info("DeduplicationCache initialized: maxSize=100000, TTL=1h");
    }

    






    public boolean isDuplicate(String eventId) {
        if (eventId == null) return false;
        boolean[] wasAbsent = {false};
        cache.get(eventId, k -> {
            wasAbsent[0] = true;
            return Boolean.TRUE;
        });
        return !wasAbsent[0];
    }

    


    public long size() {
        return cache.estimatedSize();
    }
}
