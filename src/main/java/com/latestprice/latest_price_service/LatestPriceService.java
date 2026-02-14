package com.latestprice.latest_price_service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LatestPriceService {
	
	private static final int MAX_CHUNK_SIZE = 1000;
	
	private volatile Map<String, PriceRecord> currentPrices = new ConcurrentHashMap<>();
	
	private final Map<UUID, Map<String, PriceRecord>> activeBatches = new ConcurrentHashMap<>();
	
	private final ReentrantLock batchLock = new ReentrantLock();
	
	
	public UUID startBatch() {
		
		UUID batchId = UUID.randomUUID();
		batchLock.lock();
		try {
			activeBatches.put(batchId, new ConcurrentHashMap<>());
		}finally {
			batchLock.unlock();
		}
		return batchId;
		
	}
	
	public void uploadPrices(UUID batchId,List<PriceRecord> records) {
	
		if(records == null) {
			throw new IllegalArgumentException("Records list cannot be null");
		}
		
		if(records.size() > MAX_CHUNK_SIZE) {
			throw new IllegalArgumentException("Chunk size exceeds maximum of" + MAX_CHUNK_SIZE);
		}
		
		Map<String, PriceRecord> batch = activeBatches.get(batchId);
		if(batch == null) {
			throw new IllegalStateException("Batch not started.");
		}
		
		for(PriceRecord record : records) {
			batch.merge(record.getId(), 
					record, 
					(existing, incoming) -> incoming.getAsOf().isAfter(existing.getAsOf()) ? incoming : existing);
		}
	}
	
	public void completeBatch(UUID batchId) {
		batchLock.lock();
		try {
			
			Map<String, PriceRecord> batch = activeBatches.remove(batchId);
			
			if(batch == null ) {
				throw new IllegalStateException("Batch not found");
			}
			
			Map<String, PriceRecord> newSnapshot = new ConcurrentHashMap<>(currentPrices);
			for(PriceRecord record : batch.values()) {
				newSnapshot.merge(record.getId(), record, 
						(existing,incoming) -> incoming.getAsOf().isAfter(existing.getAsOf()) ? incoming : existing);
			}
			currentPrices = newSnapshot;
									
		}finally {
			batchLock.unlock();
		}
		
	}
	
	public void cancelBatch(UUID batchId) {
		activeBatches.remove(batchId);
	}
	
	
	public Map<String, PriceRecord> getLatestPrices(List<String> ids){
		Map<String, PriceRecord> snapshot = currentPrices;
		Map<String, PriceRecord>  result = new HashMap<>();
		for(String id : ids) {
			PriceRecord record = snapshot.get(id);
			if(record != null)
				result.put(id, record);
		}
		return result;
	}
	
			

}
