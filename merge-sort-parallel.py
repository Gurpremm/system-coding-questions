
# Parallel Merge Sort: Implement the merge sort algorithm using multithreading to take 
# advantage of parallelism.

import threading

# Global lock for synchronization
lock = threading.Lock()

def merge_sort(arr):
    if len(arr) <= 1:
        return arr

    # Divide the array into two halves
    mid = len(arr) // 2
    left_half = arr[:mid]
    right_half = arr[mid:]

    # Create threads for sorting the two halves
    left_thread = threading.Thread(target=merge_sort, args=(left_half,))
    right_thread = threading.Thread(target=merge_sort, args=(right_half,))

    # Start the threads
    left_thread.start()
    right_thread.start()

    # Wait for the threads to finish
    left_thread.join()
    right_thread.join()

    # Merge the sorted halves
    merge(arr, left_half, right_half)

def merge(arr, left_half, right_half):
    i = j = k = 0

    # Acquire lock before writing to original array
    lock.acquire()

    # Merge the two sorted halves into the original array
    while i < len(left_half) and j < len(right_half):
        if left_half[i] < right_half[j]:
            arr[k] = left_half[i]
            i += 1
        else:
            arr[k] = right_half[j]
            j += 1
        k += 1

    # Copy remaining elements of left_half
    while i < len(left_half):
        arr[k] = left_half[i]
        i += 1
        k += 1

    # Copy remaining elements of right_half
    while j < len(right_half):
        arr[k] = right_half[j]
        j += 1
        k += 1

    # Release lock after finishing write operations
    lock.release()

# Example usage
if __name__ == "__main__":
    arr = [12, 11, 13, 5, 6, 7]
    print("Original array:", arr)
    merge_sort(arr)
    print("Sorted array:", arr)