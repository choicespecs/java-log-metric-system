package com.metrics.processor.repository;

import com.metrics.processor.model.ServiceMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ServiceMetadataRepository extends JpaRepository<ServiceMetadata, String> {
}
