using System;
using System.Threading;
using System.Threading.Tasks;

namespace S3BufferedUploads;

/// <summary>
/// This class implements an async-friendly locking mechanism which accepts
/// either a void or typed/generic callback
/// </summary>
public class SemaphoreLocker
{
    /// <summary>
    /// Semaphore
    /// </summary>
    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

    /// <summary>
    /// Default timeout in milliseconds (1 minute)
    /// </summary>
    public const int DEFAULT_SEMAPHORE_TIMEOUT = 60000;

    /// <summary>
    /// Timeout
    /// </summary>
    /// <value>Timeout</value>
    public int SemaphoreTimeout { get; private set; }

    /// <summary>
    /// Initializes a new <see cref="SemaphoreLocker"/> instance
    /// </summary>
    /// <param name="semaphoreTimeout">Timeout in milliseconds (default: 1 minute)</param>
    public SemaphoreLocker(int semaphoreTimeout = DEFAULT_SEMAPHORE_TIMEOUT)
    {
        if (semaphoreTimeout < 1)
        {
            throw new ArgumentException("Semaphore timeout must be greater than zero", paramName: nameof(semaphoreTimeout));
        }
        SemaphoreTimeout = semaphoreTimeout;
    }

    /// <summary>
    /// Lock the thread contet for the worker specified in the callback
    /// </summary>
    /// <param name="worker">Worker function</param>
    /// <exception cref="TimeoutException">Timeout</exception>
    public async Task LockAsync(Func<Task> worker)
    {
        if (!await _semaphore.WaitAsync(SemaphoreTimeout))
        {
            throw new TimeoutException("Unable to lock context");
        }
        try
        {
            await worker();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Lock the thread contet for the typed result worker specified in the callback
    /// </summary>
    /// <typeparam name="T">Result type</typeparam>
    /// <param name="worker">Worker function</param>
    /// <returns>Worker function result</returns>
    /// <exception cref="TimeoutException">Timeout</exception>
    public async Task<T> LockAsync<T>(Func<Task<T>> worker)
    {
        if (!await _semaphore.WaitAsync(SemaphoreTimeout))
        {
            throw new TimeoutException("Unable to lock context");
        }
        try
        {
            return await worker();
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
