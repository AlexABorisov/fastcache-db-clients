package com.fastcache.utils;

import com.fastcache.grpc.BatchValueResponse;
import com.fastcache.grpc.BoolResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamBatchObserver extends CompletableFutureObserver<BatchValueResponse,List<byte[]>> {

    public StreamBatchObserver(CompletableFuture<List<byte[]>> future) {
        super(future, res -> res.getValueList().stream().map(CompressionUtils::decompressIfNeeded).toList());
        value = new ArrayList<>();
    }

    @Override
    public void onNext(BatchValueResponse value) {
        this.value.addAll(function.apply(value));
    }

    public static class BooleanObserver extends CompletableFutureObserver<BoolResponse, List<Boolean>> {

        public BooleanObserver(CompletableFuture<List<Boolean>> future) {
            super(future, res -> List.of(res.getValue()));
        }

        @Override
        public void onNext(BoolResponse value) {
            this.value.addAll(function.apply(value));
        }

    }
}