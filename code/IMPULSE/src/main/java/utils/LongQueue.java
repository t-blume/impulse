package main.java.utils;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Stack;

public class LongQueue<T> {
    private Stack<Queue<T>> fifoQueues;

    public LongQueue() {
        fifoQueues = new Stack<>();
        fifoQueues.add(new ArrayDeque<>());
    }


    public long size() {
        long size = 0L;
        for (Queue<T> queue : fifoQueues)
            size += queue.size();
        return size;
    }

    public void add(T resource) {
        //start from last queue
        if (fifoQueues.get(fifoQueues.size() - 1).size() >= 2147480000) {
            //last queue is already full
            ArrayDeque<T> newQueue = new ArrayDeque<>();
            newQueue.add(resource);
            fifoQueues.push(newQueue);
        } else
            fifoQueues.get(fifoQueues.size() - 1).add(resource);
    }


    public T poll() {
        //start from last queue
        if (fifoQueues.peek().size() <= 0) {
            //last queue is empty, remove it
            fifoQueues.pop();
        }
        //if there is still a queue, return an element from it
        if (fifoQueues.size() > 0)
            return fifoQueues.peek().poll();
        else
            return null;
    }


    public LongQueue<T> clone(){
        LongQueue<T> clone = new LongQueue<>();
        for (Queue<T> queue : this.fifoQueues){
            Queue<T> cloneQueue = new ArrayDeque<>();
            for (T element : queue)
                cloneQueue.add(element);
            clone.fifoQueues.add(cloneQueue);

        }
        return clone;
    }
}
