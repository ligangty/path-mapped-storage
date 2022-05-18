package org.commonjava.storage.pathmapped.pathdb.datastax.util;

import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is to run non-critical pathDB jobs on backend if 'async_worker_enabled' is true (default false).
 */
public class AsyncJobExecutor {

    // properties
    public static final String PROP_ASYNC_WORKER_ENABLED = "async_worker_enabled";

    public static final String PROP_ASYNC_THREADS = "async_threads";

    // default values
    private static final int DEFAULT_ASYNC_THREADS = 4;

    private static final int DEFAULT_ASYNC_SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private ExecutorService executorService;

    public AsyncJobExecutor(PathMappedStorageConfig config)
    {
        Object p = config.getProperty( PROP_ASYNC_WORKER_ENABLED );
        if ( p != null && Boolean.parseBoolean(p.toString()))
        {
            int threads = DEFAULT_ASYNC_THREADS;
            Object t = config.getProperty( PROP_ASYNC_THREADS );
            if ( t != null )
            {
                threads = Integer.parseInt(t.toString());
            }
            logger.info("Create AsyncJobExecutor with {} threads.", threads);
            executorService = Executors.newFixedThreadPool( threads );
        }
    }

    /*
     * Blocks until all tasks have completed execution after a shutdown, or the timeout occurs,
     * or the current thread is interrupted, whichever happens first.
     */
    public void shutdownAndWaitTermination()
    {
        if ( executorService != null ) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(DEFAULT_ASYNC_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("shutdownAndWaitTermination", e);
            }
        }
    }

    public void execute(Runnable runnable) {
        if ( executorService != null ) {
            executorService.execute(runnable);
        } else {
            runnable.run();
        }
    }
}
