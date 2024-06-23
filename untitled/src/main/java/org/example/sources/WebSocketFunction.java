package org.example.sources;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.commons.math3.linear.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;

public class MySource implements SourceFunction<String> {
    private static final long serialVersionUID = 3978123556403297086L;
    
    private volatile boolean isRunning = true;
    private final String url;
    private transient WebSocket ws;
    public MySource(String url) {
        this.url = url;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // Create and configure an HttpClient
        HttpClient client = HttpClient.newHttpClient();

        // Define the WebSocket URI
        URI uri = URI.create(this.url);

        // Create and configure the WebSocket
        ws = client.newWebSocketBuilder()
                .buildAsync(uri, new WebSocket.Listener(){

                    @Override
                    public void onOpen(WebSocket webSocket) {
                        System.out.println("onOpen using subprotocol " + webSocket.getSubprotocol());
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        ctx.collect(data.toString());
                        return WebSocket.Listener.super.onText(webSocket, data, last);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                        System.out.println("Error: " + error.getMessage());
                        error.printStackTrace();
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        System.out.println("WebSocket closed with status " + statusCode + " and reason: " + reason);
                        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                    }

                    @Override
                    public CompletionStage<?> onPing(WebSocket webSocket, java.nio.ByteBuffer message) {
                        System.out.println("Ping: " + new String(message.array()));
                        return WebSocket.Listener.super.onPing(webSocket, message);
                    }

                    @Override
                    public CompletionStage<?> onPong(WebSocket webSocket, java.nio.ByteBuffer message) {
                        System.out.println("Pong: " + new String(message.array()));
                        return WebSocket.Listener.super.onPong(webSocket, message);
                    }

                })
                .join();

        while(isRunning) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        ws.abort();
    }
}
