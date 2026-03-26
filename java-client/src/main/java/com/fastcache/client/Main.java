package com.fastcache.client;

import com.fastcache.grpc.KeyHintResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static final String TEST_QUEUE = "testQueue";
    public static final int iterations = 1000000;
    public static final int sleep = 100;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        FastCacheAsyncClient client = new FastCacheAsyncClient("127.0.0.1", 50000);
        KeyHintResponse testQueue = client.createQueueAsync(TEST_QUEUE).get();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.submit(new QProducer());
        executorService.submit(new QConsumer());
        executorService.submit(new QConsumer());


    }

    static class QConsumer implements Runnable{
        FastCacheAsyncClient client = new FastCacheAsyncClient("127.0.0.1", 50001);
        static AtomicInteger CN = new AtomicInteger(0);
        @Override
        public void run() {
            int l = iterations;
            int i = CN.incrementAndGet();
            while ((l--) != 0){
                byte[] bytes = null;
                try {
                    bytes = client.getAndRemoveFrontAsync(TEST_QUEUE).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                System.out.println("consumer="+i + " "+new String(bytes));
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class QProducer implements Runnable{
        FastCacheAsyncClient client = new FastCacheAsyncClient("127.0.0.1", 50002);

        @Override
        public void run() {
            AtomicInteger integer = new AtomicInteger(0);
            int l = iterations;
            while ((l--) != 0){
                try {
                    boolean value = client.addElementToTailAsync(TEST_QUEUE,
                                                                 List.of(("val-" + integer.incrementAndGet()).getBytes()))
                            .get()
                            .getValue();
                    if (!value){
                        System.err.println("Error");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }}
    }
}