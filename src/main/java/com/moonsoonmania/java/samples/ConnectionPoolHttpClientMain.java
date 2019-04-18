package com.moonsoonmania.java.samples;


import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.*;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;

class MainConfig {

    static Integer TotalCount  = 1024*256;

    static Integer MaxOpenRequests = 2048;

    static Boolean PrintResponse = false;

    static Integer  MaxConnections = 2048;

}

class CustomThreadFactory implements ThreadFactory {

    public Thread newThread(Runnable r) {
        return new Thread(r);
    }
}

class RecordingResponseHandler implements SdkAsyncHttpResponseHandler {

    private List<SdkHttpResponse> responses = new ArrayList<>();
    private StringBuilder bodyParts = new StringBuilder();
    CompletableFuture<String> completeFuture = new CompletableFuture<>();

    @Override
    public void onHeaders(SdkHttpResponse response) {
        responses.add(response);
    }

    @Override
    public void onStream(Publisher<ByteBuffer> publisher) {
        publisher.subscribe(new SimpleSubscriber(byteBuffer -> {
            byte[] b = new byte[byteBuffer.remaining()];
            byteBuffer.duplicate().get(b);
            bodyParts.append(new String(b, StandardCharsets.UTF_8));
        }) {

            @Override
            public void onError(Throwable t) {
                completeFuture.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                completeFuture.complete(bodyParts.toString());
            }
        });
    }

    @Override
    public void onError(Throwable error) {
        completeFuture.completeExceptionally(error);

    }

}

class ConnectionPoolHttpClient {

    private static final String URL = System.getProperty("url", "http://127.0.0.1/");

    private SdkAsyncHttpClient client;

    private AtomicInteger totalRequestCount = new AtomicInteger(0);
    private AtomicInteger totalResponseCount = new AtomicInteger(0);
    private AtomicInteger totalResponseSuccessCount = new AtomicInteger(0);
    private AtomicInteger totalResponseErrorCount = new AtomicInteger(0);
    private AtomicInteger outstandingRequestCount = new AtomicInteger(0);

    private Long startTime = 0L;

    ConnectionPoolHttpClient() {
        ThreadFactory threadFactory = new CustomThreadFactory();
        this.client = NettyNioAsyncHttpClient.builder()
                .eventLoopGroupBuilder(SdkEventLoopGroup.builder()
                        .threadFactory(threadFactory))
                .maxConcurrency(MainConfig.MaxConnections)
                .build();
        System.out.println("Initialized connection pool...");
    }

    private SdkHttpFullRequest createRequest(URI uri) {
        return createRequest(uri, "/", null, SdkHttpMethod.GET, emptyMap());
    }

    private SdkHttpFullRequest createRequest(URI uri,
                                             String resourcePath,
                                             String body,
                                             SdkHttpMethod method,
                                             Map<String, String> params) {
        String contentLength = body == null ? null : String.valueOf(body.getBytes(UTF_8).length);
        return SdkHttpFullRequest.builder()
                .uri(uri)
                .method(method)
                .encodedPath(resourcePath)
                .applyMutation(b -> params.forEach(b::putRawQueryParameter))
                .applyMutation(b -> {
                    b.putHeader("Host", uri.getHost());
                    if (contentLength != null) {
                        b.putHeader("Content-Length", contentLength);
                    }
                }).build();
    }

    private static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    private static Collection<String> splitStringBySize(String str) {
        if (isBlank(str)) {
            return Collections.emptyList();
        }
        ArrayList<String> split = new ArrayList<>();
        for (int i = 0; i <= str.length() / 1000; i++) {
            split.add(str.substring(i * 1000, Math.min((i + 1) * 1000, str.length())));
        }
        return split;
    }

    private SdkHttpContentPublisher createProvider(String body) {
        Stream<ByteBuffer> chunks = splitStringBySize(body).stream()
                .map(chunk -> ByteBuffer.wrap(chunk.getBytes(UTF_8)));
        return new SdkHttpContentPublisher() {

            @Override
            public Optional<Long> contentLength() {
                return Optional.of((long) body.length());
            }

            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        chunks.forEach(s::onNext);
                        s.onComplete();
                    }

                    @Override
                    public void cancel() {

                    }
                });
            }
        };
    }

    private RecordingResponseHandler makeSimpleRequest() {
        String body = "";
        URI uri = URI.create(URL);
        SdkHttpRequest request = createRequest(uri);
        RecordingResponseHandler recorder = new RecordingResponseHandler();

        AsyncExecuteRequest executeRequest = AsyncExecuteRequest.builder()
                .request(request)
                .requestContentPublisher(createProvider(body))
                .responseHandler(recorder)
                .build();

        client.execute(executeRequest);

        return recorder;
    }

    private void handleResponse(String result) {
        if(null != result) {
            if(MainConfig.PrintResponse) System.out.println(result);
            totalResponseSuccessCount.getAndIncrement();
        } else {
            totalResponseErrorCount.getAndIncrement();
        }

        outstandingRequestCount.getAndDecrement();

        int count = totalResponseCount.getAndIncrement();

        if(count > 0 && count % 10000 == 0) {
            Long endTime = System.currentTimeMillis();

            long elapsed = endTime - startTime;
            float perSecond = count / ((float) elapsed / 1000);
            System.out.println(Timestamp.from(Instant.now()) + " - Processed " + count +
                    " responses. - (" + elapsed + "ms @ " + perSecond + " req/s)");
        }

        if(totalResponseCount.get() == MainConfig.TotalCount) {
            Long endTime = System.currentTimeMillis();

            long elapsed = endTime - startTime;

            System.out.println("Requested Count: " + totalRequestCount.get());
            System.out.println("Responses Count: " + totalResponseCount.get());
            System.out.println("    Success Count: " + totalResponseSuccessCount.get());
            System.out.println("    Error Count: " + totalResponseErrorCount.get());
            System.out.println(Timestamp.from(Instant.now()) + " - Elapsed: " + elapsed + " ms");

            System.exit(0);
        } else {
            sendRequest();
        }
    }

    private void sendRequest() {

        if(totalRequestCount.get() != MainConfig.TotalCount) {

            outstandingRequestCount.getAndIncrement();

            totalRequestCount.getAndIncrement();

            RecordingResponseHandler recorder = makeSimpleRequest();

            recorder.completeFuture.whenCompleteAsync((result, throwable) -> {
                if (throwable == null) {
                    handleResponse(result);
                } else {
                    handleResponse(null);
                }
            });
        }
    }

    void bootstrap() {

        startTime = System.currentTimeMillis();
        System.out.println(Timestamp.from(Instant.now()) + " - Start");
        while(outstandingRequestCount.get() < MainConfig.MaxOpenRequests) {
            sendRequest();
        }

    }


}

public final class ConnectionPoolHttpClientMain {

    public static void main(String[] args) {

        ConnectionPoolHttpClient client = new ConnectionPoolHttpClient();
        client.bootstrap();

//        RecordingResponseHandler recorder = client.makeSimpleRequest();
//
//        recorder.completeFuture.whenCompleteAsync((result, throwable) -> {
//            if(throwable == null) {
//                System.out.println(result);
//            }
//        });


    }

}
