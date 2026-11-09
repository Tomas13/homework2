# gRPC Ping/Pong

A gRPC server (Java) on port 8080 plus two clients (Java and Python). All three are generated from the same proto file: `proto/ping.proto`.

```
proto/ping.proto        shared contract (PingService.Ping: PingRequest -> PingResponse)
java/                   Gradle project: PingServer + PingClient
python-client/          Python client (stubs generated from ../proto/ping.proto)
```

## Requirements
- Java 17 (Gradle comes from the wrapper, and protoc is downloaded automatically)
- Python 3.9+

## 1. Start the server
```bash
cd java
./gradlew run
# gRPC server started, listening on port 8080
```

## 2. Java client (in a second terminal)
```bash
cd java
./gradlew runClient                                 # default localhost:8080
./gradlew runClient -PclientArgs="localhost 8080"   # explicit host/port
```

## 3. Python client
```bash
cd python-client
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./generate.sh                  # generates ping_pb2.py / ping_pb2_grpc.py from ../proto/ping.proto
.venv/bin/python client.py     # optional args: host port
```

## Example output
Java client:
```
Sending: Ping
Received response:
  message:   Pong
  timestamp: 1794229356955
  server:    java-ping-server
```
Python client:
```
Sending: Ping
Received response:
  message:   Pong
  timestamp: 1794229360694
  server:    java-ping-server
```
Server:
```
gRPC server started, listening on port 8080
Received request: message="Ping", client="java"
Received request: message="Ping", client="python"
```
