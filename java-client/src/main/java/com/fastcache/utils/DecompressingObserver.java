package com.fastcache.utils;

import com.fastcache.grpc.UpdateValueResponse;
import com.fastcache.grpc.ValueResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DecompressingObserver extends CompletableFutureObserver<ValueResponse, byte[]> {

    public DecompressingObserver(CompletableFuture<byte[]> future) {
        super(future, CompressionUtils::decompressIfNeeded);
    }

    public static class Update extends CompletableFutureObserver<UpdateValueResponse, byte[]> {

        public Update(CompletableFuture<byte[]> future) {
            super(future, CompressionUtils::decompressIfNeeded);
        }
    }
}