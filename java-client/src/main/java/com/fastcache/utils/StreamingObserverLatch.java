package com.fastcache.utils;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public record StreamingObserverLatch<T>(Consumer<T> streamConsumer, CountDownLatch latch) implements StreamObserver<T> {
    @Override
    public void onNext(T value) {
        streamConsumer.accept(value);
    }

    @Override
    public void onError(Throwable t) {
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }
}