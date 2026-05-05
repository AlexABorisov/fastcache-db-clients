package com.fastcache.utils;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class CompletableFutureObserver<T,VAL> implements StreamObserver<T> {
    protected final CompletableFuture<VAL> future;
    protected final Function<T, VAL> function;
    protected VAL value;


    public CompletableFutureObserver(CompletableFuture<VAL> future){
        this(future,t -> (VAL)t);
    }
    public CompletableFutureObserver(CompletableFuture<VAL> future, Function<T,VAL> function) {
        this.future = future;
        this.function = function;
    }

    @Override
    public void onNext(T value) {
        this.value = function.apply(value);
    }

        @Override
    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        future.complete(value);
    }
}