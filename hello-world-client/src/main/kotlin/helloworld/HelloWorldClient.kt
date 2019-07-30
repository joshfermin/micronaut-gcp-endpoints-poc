package helloworld

import io.grpc.ClientInterceptor
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.CountDownLatch
import io.grpc.stub.StreamObserver
import io.grpc.StatusRuntimeException
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannel
import io.grpc.Status
import mu.KotlinLogging
import io.grpc.ForwardingClientCall
import io.grpc.ClientCall
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException


class HelloWorldClient(name: String, apiKey: String) {
    companion object {
        private val logger = KotlinLogging.logger {}

        /** Issues several different requests and then exits.  */
        @Throws(InterruptedException::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val options = createOptions()
            val parser = DefaultParser()
            val params: CommandLine
            try {
                params = parser.parse(options, args)
            } catch (e: ParseException) {
                System.err.println("Invalid command line: $e")
                return
            }

            val address = params.getOptionValue("host")
            val apiKey = params.getOptionValue("api_key")

            val client = HelloWorldClient(address, apiKey)
            try {
                client.sayHello("Test")
                client.lotsOfReplies("Test")
                client.lotsOfGreetings(listOf("Test", "Other Test", "Second Test"))
                val finishLatch = client.bidiHello()
                if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                    logger.warn("bidirectional test did not finish within 1 minutes")
                }
            } finally {
                client.shutdown()
            }
        }

        private fun createOptions(): Options {
            val options = Options()

            options.addOption(Option.builder()
                    .longOpt("host")
                    .desc("The address of the gRPC server")
                    .hasArg()
                    .argName("host")
                    .type(String::class.java)
                    .build())

            options.addOption(Option.builder()
                    .longOpt("api_key")
                    .desc("The API key to use for RPC calls")
                    .hasArg()
                    .argName("key")
                    .type(String::class.java)
                    .build())

            return options
        }
    }

    private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(name).usePlaintext().build()
    private val ch: Channel = ClientInterceptors.intercept(channel, Interceptor(apiKey))
    private val blockingStub: GreeterGrpc.GreeterBlockingStub = GreeterGrpc.newBlockingStub(ch)
    private var asyncStub: GreeterGrpc.GreeterStub = GreeterGrpc.newStub(ch)

    private var random = Random()

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    /**
     * Blocking unary call example.  Calls sayHello and prints the response.
     */
    private fun sayHello(name: String) {
        val request = HelloRequest.newBuilder().setName(name).build()

        val response: HelloReply
        try {
            response = blockingStub.sayHello(request)
        } catch (e: StatusRuntimeException) {
            logger.warn("RPC failed: {0}", e.status)
            return
        }

        logger.info(response.message)
    }

    /**
     * Blocking server-streaming example. Calls listFeatures with a rectangle of interest. Prints each
     * response feature as it arrives.
     */
    fun lotsOfReplies(name: String) {
        val request = HelloRequest.newBuilder().setName(name).build()
        val features: Iterator<HelloReply>
        try {
            features = blockingStub.lotsOfReplies(request)
            var i = 1
            while (features.hasNext()) {
                val feature = features.next()
                logger.info("Result #$i: {0}", feature)
                i++
            }
        } catch (e: StatusRuntimeException) {
            logger.warn("RPC failed: {0}", e.status)
        }
    }

    /**
     * Async client-streaming example.
     */
    @Throws(InterruptedException::class)
    fun lotsOfGreetings(features: List<String>) {
        val finishLatch = CountDownLatch(1)
        val responseObserver = object : StreamObserver<HelloReply> {
            override fun onNext(value: HelloReply) {
                logger.info(value.message)
            }

            override fun onError(t: Throwable) {
                logger.warn("RecordRoute Failed: {0}", Status.fromThrowable(t))
                finishLatch.countDown()
            }

            override fun onCompleted() {
                logger.info("Finished RecordRoute")
                finishLatch.countDown()
            }
        }

        val requestObserver = asyncStub.lotsOfGreetings(responseObserver)
        try {
            // Send numPoints points randomly selected from the features list.
            for (name in features) {
                requestObserver.onNext(HelloRequest.newBuilder().setName("Name: $name").build())
                // Sleep for a bit before sending the next one.
                Thread.sleep((random.nextInt(1000) + 500).toLong())
                if (finishLatch.count == 0L) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    return
                }
            }
        } catch (e: RuntimeException) {
            // Cancel RPC
            requestObserver.onError(e)
            throw e
        }

        // Mark the end of requests
        requestObserver.onCompleted()

        // Receiving happens asynchronously
        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warn("client streaming did not finish within 1 minute")
        }
    }

    /**
     * Bi-directional example, which can only be asynchronous. Send some chat messages, and print any
     * chat messages that are sent from the server.
     */
    fun bidiHello(): CountDownLatch {
        val finishLatch = CountDownLatch(1)
        val requestObserver = asyncStub.bidiHello(object : StreamObserver<HelloReply> {
            override fun onNext(message: HelloReply) {
                logger.info("Got message ${message.message}")
            }

            override fun onError(t: Throwable) {
                logger.warn("bidirectional Failed: {0}", Status.fromThrowable(t))
                finishLatch.countDown()
            }

            override fun onCompleted() {
                logger.info("Finished bidirectional")
                finishLatch.countDown()
            }
        })

        try {
            val requests = listOf("Test", "Other Test", "Another Test")

            for (request in requests) {
                val helloRequest = HelloRequest.newBuilder().setName(request).build()
                requestObserver.onNext(helloRequest)
            }
        } catch (e: RuntimeException) {
            // Cancel RPC
            requestObserver.onError(e)
            throw e
        }

        // Mark the end of requests
        requestObserver.onCompleted()

        // return the latch while receiving happens asynchronously
        return finishLatch
    }

    private class Interceptor(private val apiKey: String?) : ClientInterceptor {
        override fun <ReqT, RespT> interceptCall(
                method: MethodDescriptor<ReqT, RespT>, callOptions: CallOptions, next: Channel): ClientCall<ReqT, RespT> {
            LOGGER.info("Intercepted " + method.fullMethodName)
            var call = next.newCall(method, callOptions)
            call = object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
                override fun start(responseListener: ClientCall.Listener<RespT>, headers: Metadata) {
                    if (apiKey != null && !apiKey.isEmpty()) {
                        LOGGER.info("Attaching API Key: $apiKey")
                        headers.put(API_KEY_HEADER, apiKey)
                    }
                    super.start(responseListener, headers)
                }
            }
            return call
        }
        companion object {
            private val LOGGER = KotlinLogging.logger {}
            private val API_KEY_HEADER = Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)
        }
    }
}
