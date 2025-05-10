package org.gridsuite.cgmes.assembling.job;

import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.ExceptionThrowingConsumer;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.Times;
import org.springframework.lang.NonNull;

import static com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.withSftpServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public final class TestUtils {
    public static final String SFTP_LABEL = "my_sftp_server";

    private TestUtils() {
        throw new IllegalCallerException("Utility class");
    }

    public static void withSftp(@NonNull final ExceptionThrowingConsumer testCode) throws Exception {
        withSftpServer(server -> {
            server.addUser("dummy", "dummy")/*.setPort(2222)*/;
            server.createDirectory("cases");
            testCode.accept(server);
            server.deleteAllFilesAndDirectories();
        });
    }

    public static void expectRequestGet(MockServerClient mockServerClient, String path, String response, Integer status) {
        mockServerClient.when(request().withMethod("GET").withPath(path), Times.exactly(1))
                .respond(response().withStatusCode(status).withBody(response));
    }

    public static void expectRequestPost(MockServerClient mockServerClient, String path, Integer status) {
        mockServerClient.when(request().withMethod("POST").withPath(path), Times.exactly(1))
                .respond(response().withStatusCode(status));
    }
}
