package helloworld

import io.grpc.Status
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import javax.inject.Singleton

@Singleton
class GreetingEndpoint(private val greetingService: GreetingService) : GreeterGrpc.GreeterImplBase() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun lotsOfReplies(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        val message = greetingService.sayHello(request.name)
        for (i in 1..5) {
            val reply = HelloReply.newBuilder().setMessage("$message - $i").build()
            responseObserver.onNext(reply)
        }
        responseObserver.onCompleted()
    }

    override fun lotsOfGreetings(responseObserver: StreamObserver<HelloReply>): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
            private var count = 0
            private var name: String = ""
            override fun onNext(value: HelloRequest) {
                name = value.name
                count++
            }

            override fun onError(t: Throwable?) {
                logger.warn("client streaming Failed: {0}", Status.fromThrowable(t))

            }

            override fun onCompleted() {
                val message = greetingService.sayHello("Count is: $count, Name is: $name")
                responseObserver.onNext(HelloReply.newBuilder().setMessage(message).build())
                responseObserver.onCompleted()
            }

        }
    }

    override fun bidiHello(responseObserver: StreamObserver<HelloReply>): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
            private var count = 0
            override fun onNext(value: HelloRequest?) {
                count++
                val message = greetingService.sayHello("Count is: $count")
                responseObserver.onNext(HelloReply.newBuilder().setMessage(message).build())
            }

            override fun onError(t: Throwable?) {
                logger.warn("bidirectional failed: {0}", Status.fromThrowable(t))
            }

            override fun onCompleted() {
                responseObserver.onCompleted()
            }

        }
    }

    override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        val message = greetingService.sayHello(request.name)
        val reply = HelloReply.newBuilder().setMessage(message).build()
        responseObserver.onNext(reply)
        responseObserver.onCompleted()
    }
}

