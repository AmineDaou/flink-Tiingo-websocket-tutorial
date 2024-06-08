package org.example.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.java_websocket.client.WebSocketClient;

import java.net.URI;
import java.net.URISyntaxException;


public class WebSocketFunction implements SourceFunction<String> {
    private static final long serialVersionUID = 3978123556403297086L;

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketFunction.class);
    private final URI url;
    private WebSocketClient wsClient;
    private volatile boolean isRunning = true;
    public WebSocketFunction(String url) throws URISyntaxException {
        this.url = new URI(url);
    }

    @Override
    public void run(SourceContext<String> ctx) {
        while(isRunning){
            ctx.collect("etetetet");
            LOG.info("ggggggggggggg");
            System.out.println("gggjhiuyuy");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
