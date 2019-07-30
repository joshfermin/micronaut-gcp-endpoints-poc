package helloworld

import com.google.common.annotations.VisibleForTesting
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.CountDownLatch
import java.io.IOException
import io.grpc.stub.StreamObserver
import io.grpc.StatusRuntimeException
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannel
import io.grpc.Status
import mu.KotlinLogging
import javax.inject.Singleton

@Singleton
class GreetingClient(name: String, port: Int) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(name, port).build()
    private val blockingStub: GreeterGrpc.GreeterBlockingStub = GreeterGrpc.newBlockingStub(channel)
    private var asyncStub: GreeterGrpc.GreeterStub = GreeterGrpc.newStub(channel)

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
            logger.warn("client streaming can not finish within 1 minutes")
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

    /** Issues several different requests and then exits.  */
    @Throws(InterruptedException::class)
    fun main(args: Array<String>) {
        val client = GreetingClient("34.66.126.97", 80)
        try {
            // Looking for a valid feature
            client.sayHello("Rener")

            // Feature missing.
            client.lotsOfReplies("Joe")

            // Looking for features between 40, -75 and 42, -73.
            client.lotsOfGreetings(listOf("Test", "Other Test", "Second Test"))

            // Send and receive some notes.
            val finishLatch = client.bidiHello()

            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                logger.warn("routeChat can not finish within 1 minutes")
            }
        } finally {
            client.shutdown()
        }
    }

    /**
     * Only used for unit test, as we do not want to introduce randomness in unit test.
     */
    @VisibleForTesting
    fun setRandom(random: Random) {
        this.random = random
    }
}
