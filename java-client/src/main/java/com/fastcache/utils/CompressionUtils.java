package com.fastcache.utils;

import com.fastcache.grpc.BinaryPayload;
import com.fastcache.grpc.CompressedInfo;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.UpdateValueResponse;
import com.fastcache.grpc.Value;
import com.fastcache.grpc.ValueResponse;
import com.google.protobuf.ByteString;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class CompressionUtils {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final int COMPRESSION_THRESHOLD = 1024; // 1KB


    public static Key.Builder compressKeyIfNeeded(byte[] data,Integer clientId) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        Key.Builder keyBuilder = Key.newBuilder();

        if (data.length > COMPRESSION_THRESHOLD) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

            keyBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                    .setEnabled(true)
                                                    .setRawSize(data.length)
                                                    .build());
        } else {
            payloadBuilder.setPayload(ByteString.copyFrom(data));
            payloadBuilder.setSize(data.length);
        }
        if (clientId != null){
            keyBuilder.setClientId(clientId);
        }

        return keyBuilder.setPayload(payloadBuilder.build());
    }

    public static Value compressIfNeeded(byte[] data) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();

        if (data.length > COMPRESSION_THRESHOLD) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

            valueBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                    .setEnabled(true)
                                                    .setRawSize(data.length)
                                                    .build());
        } else {
            payloadBuilder.setPayload(ByteString.copyFrom(data));
            payloadBuilder.setSize(data.length);
        }

        return valueBuilder.setValue(payloadBuilder.build()).build();
    }

    public static byte[] decompressIfNeeded(ValueResponse responseValue){
        return decompressIfNeeded(responseValue.getValue());
    }
    public static byte[] decompressIfNeeded(UpdateValueResponse responseValue){
        return decompressIfNeeded(responseValue.getValue());
    }

    public static byte[] decompressIfNeeded(Value responseValue) {
        BinaryPayload payload = responseValue.getValue();
        byte[] data = payload.getPayload().toByteArray();

        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            int rawSize = responseValue.getCompressionInfo().getRawSize();
            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] restored = new byte[rawSize];
            decompressor.decompress(data, 0, data.length, restored, 0);
            return restored;
        }
        return data;
    }

}