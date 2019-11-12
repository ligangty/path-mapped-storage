package org.commonjava.storage.pathmapped.config;

import java.util.Map;

public class DefaultPathMappedStorageConfig
                implements PathMappedStorageConfig
{
    private static final int DEFAULT_GC_BATCH_SIZE = 0; // no limit

    private final int DEFAULT_GC_INTERVAL_IN_MINUTES = 60;

    private final int DEFAULT_GC_GRACE_PERIOD_IN_HOURS = 24;

    private final String DEFAULT_FILE_CHECKSUM_ALGORITHM = "MD5";

    private int gcGracePeriodInHours = DEFAULT_GC_GRACE_PERIOD_IN_HOURS;

    private int gcIntervalInMinutes = DEFAULT_GC_INTERVAL_IN_MINUTES;

    private int gcBatchSize = DEFAULT_GC_BATCH_SIZE;

    private String fileChecksumAlgorithm = DEFAULT_FILE_CHECKSUM_ALGORITHM;

    public DefaultPathMappedStorageConfig()
    {
    }

    public DefaultPathMappedStorageConfig( Map<String, Object> properties )
    {
        this.properties = properties;
    }

    @Override
    public int getGCIntervalInMinutes()
    {
        return gcIntervalInMinutes;
    }

    @Override
    public int getGCGracePeriodInHours()
    {
        return gcGracePeriodInHours;
    }

    public void setGcGracePeriodInHours( int gcGracePeriodInHours )
    {
        this.gcGracePeriodInHours = gcGracePeriodInHours;
    }

    public void setGcIntervalInMinutes( int gcIntervalInMinutes )
    {
        this.gcIntervalInMinutes = gcIntervalInMinutes;
    }

    @Override
    public String getFileChecksumAlgorithm()
    {
        return fileChecksumAlgorithm;
    }

    public void setFileChecksumAlgorithm( String fileChecksumAlgorithm )
    {
        this.fileChecksumAlgorithm = fileChecksumAlgorithm;
    }

    private Map<String, Object> properties;

    @Override
    public Object getProperty( String key )
    {
        if ( properties != null )
        {
            return properties.get( key );
        }
        return null;
    }

    @Override
    public int getGCBatchSize()
    {
        return gcBatchSize;
    }

    public void setGcBatchSize( int gcBatchSize )
    {
        this.gcBatchSize = gcBatchSize;
    }
}
