package org.commonjava.storage.pathmapped.config;

public interface PathMappedStorageConfig
{

    int getGCIntervalInMinutes();

    int getGCGracePeriodInHours();

    boolean isSubsystemEnabled( String fileSystem );

    Object getProperty( String key );

    int getGCBatchSize();
}
