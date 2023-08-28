/**
 * Copyright (C) 2019 Red Hat, Inc. (nos-devel@redhat.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.storage.pathmapped.config;

import java.util.Map;

public class DefaultPathMappedStorageConfig
                implements PathMappedStorageConfig
{
    private static final int DEFAULT_GC_BATCH_SIZE = 0; // no limit

    private static final int MAX_GC_RESULT_SIZE = 100000;

    private final int DEFAULT_GC_INTERVAL_IN_MINUTES = 60;

    private final int DEFAULT_GC_GRACE_PERIOD_IN_HOURS = 24;

    private final String DEFAULT_FILE_CHECKSUM_ALGORITHM = "SHA-256";

    private int gcGracePeriodInHours = DEFAULT_GC_GRACE_PERIOD_IN_HOURS;

    private int gcIntervalInMinutes = DEFAULT_GC_INTERVAL_IN_MINUTES;

    private int gcBatchSize = DEFAULT_GC_BATCH_SIZE;

    private int gcMaxResultSize = MAX_GC_RESULT_SIZE;

    private String fileChecksumAlgorithm = DEFAULT_FILE_CHECKSUM_ALGORITHM;

    private String deduplicatePattern;

    private static final String DEFAULT_COMMON_FILE_EXTENSIONS = ".+\\.(jar|json|xml|pom|gz|tgz|md5|sha1|sha256)$";

    private String commonFileExtensions = DEFAULT_COMMON_FILE_EXTENSIONS;

    private boolean physicalFileExistenceCheckEnabled = false;

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

    @Override
    public String getDeduplicatePattern()
    {
        return deduplicatePattern;
    }

    public void setDeduplicatePattern( String deduplicatePattern )
    {
        this.deduplicatePattern = deduplicatePattern;
    }

    @Override
    public String getCommonFileExtensions()
    {
        return commonFileExtensions;
    }

    public void setCommonFileExtensions( String commonFileExtensions )
    {
        this.commonFileExtensions = commonFileExtensions;
    }

    @Override
    public boolean isPhysicalFileExistenceCheckEnabled() {
        return physicalFileExistenceCheckEnabled;
    }

    public void setPhysicalFileExistenceCheckEnabled(boolean physicalFileExistenceCheckEnabled) {
        this.physicalFileExistenceCheckEnabled = physicalFileExistenceCheckEnabled;
    }

    @Override
    public int getGcMaxResultSize()
    {
        return gcMaxResultSize;
    }

    public void setGcMaxResultSize(int gcMaxResultSize)
    {
        this.gcMaxResultSize = gcMaxResultSize;
    }
}
