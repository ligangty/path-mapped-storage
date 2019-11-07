package org.commonjava.storage.pathmapped.config;

public interface PathMappedStorageConfig
{

    int getGCIntervalInMinutes();

    int getGCGracePeriodInHours();

    String getFileChecksumAlgorithm();

    default boolean isSubsystemEnabled() { return true; }

    Object getProperty( String key );

    int getGCBatchSize();
}
