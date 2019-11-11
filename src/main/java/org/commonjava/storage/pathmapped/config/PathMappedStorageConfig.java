package org.commonjava.storage.pathmapped.config;

public interface PathMappedStorageConfig
{

    int getGCIntervalInMinutes();

    int getGCGracePeriodInHours();

    String getFileChecksumAlgorithm();

    Object getProperty( String key );

    int getGCBatchSize();
}
