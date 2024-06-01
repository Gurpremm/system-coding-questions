# Implement following method of ScheduledExecutorService interface in Python

# public class ScheduledExecutorService {

# 	/**
# 	* Creates and executes a one-shot action that becomes enabled after the given delay.
# 	*/
# 	public void schedule(Runnable command, long delay, TimeUnit unit) {
# 	}

# 	/**
# 	* Creates and executes a periodic action that becomes enabled first after the given initial delay, and 
# 	* subsequently with the given period; that is executions will commence after initialDelay then 
# 	* initialDelay+period, then initialDelay + 2 * period, and so on.
# 	*/
# 	public void scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
# 	}

# 	/*
# 	* Creates and executes a periodic action that becomes enabled first after the given initial delay, and 
# 	* subsequently with the given delay between the termination of one execution and the commencement of the next.
# 	*/
# 	public void scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
# 	}

# }


import threading
import time
import heapq

class TimeUnit:
    SECONDS = 'seconds'
    MILLISECONDS = 'milliseconds'
    MICROSECONDS = 'microseconds'

    @staticmethod
    def to_seconds(delay, unit):
        if unit == TimeUnit.SECONDS:
            return delay
        elif unit == TimeUnit.MILLISECONDS:
            return delay / 1000.0
        elif unit == TimeUnit.MICROSECONDS:
            return delay / 1000000.0
        else:
            raise ValueError(f"Unsupported time unit: {unit}")

class ScheduledExecutorService:
    def __init__(self):
        self._shutdown = False
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._task_queue = []
        self._worker_thread = threading.Thread(target=self._run)
        self._worker_thread.start()

    def _run(self):
        while not self._shutdown:
            with self._condition:
                if not self._task_queue:
                    self._condition.wait()
                else:
                    next_run_time, command = heapq.heappop(self._task_queue)
                    now = time.time()
                    if next_run_time > now:
                        heapq.heappush(self._task_queue, (next_run_time, command))
                        self._condition.wait(timeout=next_run_time - now)
                    else:
                        threading.Thread(target=command).start()
                        self._condition.notify_all()

    def schedule(self, command, delay, unit):
        if self._shutdown:
            raise RuntimeError("ScheduledExecutorService is shutdown")
        delay_seconds = TimeUnit.to_seconds(delay, unit)
        run_time = time.time() + delay_seconds

        with self._condition:
            heapq.heappush(self._task_queue, (run_time, command))
            self._condition.notify_all()

    def scheduleAtFixedRate(self, command, initialDelay, period, unit):
        if self._shutdown:
            raise RuntimeError("ScheduledExecutorService is shutdown")
        initial_delay_seconds = TimeUnit.to_seconds(initialDelay, unit)
        period_seconds = TimeUnit.to_seconds(period, unit)

        def run_command():
            time.sleep(initial_delay_seconds)
            while not self._shutdown:
                run_time = time.time()
                self.schedule(command, 0, TimeUnit.SECONDS)
                run_time += period_seconds
                time.sleep(max(0, run_time - time.time()))

        threading.Thread(target=run_command).start()

    def scheduleWithFixedDelay(self, command, initialDelay, delay, unit):
        if self._shutdown:
            raise RuntimeError("ScheduledExecutorService is shutdown")
        initial_delay_seconds = TimeUnit.to_seconds(initialDelay, unit)
        delay_seconds = TimeUnit.to_seconds(delay, unit)

        def run_command():
            time.sleep(initial_delay_seconds)
            while not self._shutdown:
                self.schedule(command, 0, TimeUnit.SECONDS)
                time.sleep(delay_seconds)

        threading.Thread(target=run_command).start()

    def shutdown(self, wait=True):
        with self._condition:
            self._shutdown = True
            self._condition.notify_all()
        if wait:
            self._worker_thread.join()

# Example usage
if __name__ == "__main__":
    def my_task():
        print("Task executed at", time.time())

    service = ScheduledExecutorService()
    
    # Schedule a one-shot action
    service.schedule(my_task, 3, TimeUnit.SECONDS)
    
    # Schedule a periodic action at fixed rate
    service.scheduleAtFixedRate(my_task, 1, 2, TimeUnit.SECONDS)
    
    # Schedule a periodic action with fixed delay
    service.scheduleWithFixedDelay(my_task, 1, 2, TimeUnit.SECONDS)
    
    time.sleep(10)
    service.shutdown()


# Priority Queue: We use a priority queue (heapq) to manage the tasks based on their scheduled execution time. This ensures that the task with the nearest execution time is always executed first.
# Worker Thread: A single worker thread runs in the background, continuously checking and executing tasks from the priority queue.
# Condition Variable: We use a condition variable to notify the worker thread whenever a new task is added or when the service is shut down.
# schedule: Schedules a one-shot task to be executed after a given delay.
# scheduleAtFixedRate: Schedules a periodic task that starts after an initial delay and is executed at fixed intervals.
# scheduleWithFixedDelay: Schedules a periodic task that starts after an initial delay and is executed with a fixed delay between the end of one execution and the start of the next.
# shutdown: Shuts down the executor, stopping the worker thread and any further task scheduling.


# The self._condition.notify_all() method in Python's threading.Condition class is used to wake up all threads that are waiting on the condition variable. Here's a detailed explanation of how it works in the context of the ScheduledExecutorService:

# What self._condition.notify_all() Does:
# Wakes Up Waiting Threads:

# When notify_all() is called, it signals all threads that are waiting on the condition variable. This means all threads that have called self._condition.wait() will be awakened and made runnable again.
# Synchronization Mechanism:

# The condition variable is used as a synchronization mechanism to control access to shared resources. In this case, the shared resource is the task queue (self._task_queue).
# Releases and Reacquires Lock:

# When a thread calls self._condition.wait(), it releases the associated lock (self._lock) and goes to sleep until notify_all() (or notify()) is called.
# Once notify_all() is called, the waiting threads wake up, re-acquire the lock, and proceed with their operations.
# Usage in ScheduledExecutorService:
# Task Addition:

# When a new task is scheduled via the schedule, scheduleAtFixedRate, or scheduleWithFixedDelay methods, self._condition.notify_all() is called after the task is added to the priority queue. This ensures that the worker thread is notified of the new task and can adjust its wait time or execute the task if needed.
# Worker Thread:

# The worker thread continuously checks the priority queue for tasks to execute. If the queue is empty, the worker thread waits on the condition variable.
# When notify_all() is called, the worker thread is awakened and rechecks the task queue to see if there are new tasks to execute.