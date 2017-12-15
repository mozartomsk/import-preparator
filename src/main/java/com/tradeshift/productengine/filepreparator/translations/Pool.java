package com.tradeshift.productengine.filepreparator.translations;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class Pool<T extends Closeable> implements Closeable {

    private final ConcurrentLinkedQueue<T> elements = new ConcurrentLinkedQueue<>();

    private final List<T> allElements = new ArrayList<>();

    private final int maxCount;

    private final Supplier<T> supplier;

    public Pool(int maxCount, Supplier<T> supplier) {
        this.maxCount = maxCount;
        this.supplier = supplier;
    }

    public T take() {
        T result;
        while (true) {
            result = elements.poll();
            if (result != null) {
                return result;
            }
            synchronized (this) {
                if (allElements.size() < maxCount) {
                    result = supplier.get();
                    allElements.add(result);
                    return result;
                }
                try {
                    this.wait();
                } catch (InterruptedException ignore) {}
            }
        }
    }

    public void setFree(T t) {
        synchronized (this) {
            if (!allElements.contains(t)) {
                throw new IllegalStateException("Can not set free not initial object");
            }
            elements.add(t);
            this.notify();
        }
    }

    public List<T> getAll() {
        return new ArrayList<T>(allElements);
    }

    @Override
    public void close() throws IOException {
        for (T t : getAll()) {
            t.close();
        }
    }
}
