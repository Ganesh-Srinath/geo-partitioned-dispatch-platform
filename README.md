# \# Geo-Partitioned Distributed Dispatch Platform

# 

# A ride-dispatch backend built to explore real distributed systems problems: sharding, concurrency control, replication, failover, event-driven workflows, and idempotency. Built with Java 21, Spring Boot, Kafka and Docker.

# 

# Riders request rides and drivers stream their locations. The cluster matches each rider with a nearby free driver, and never gives one driver to two riders, even with hundreds of concurrent requests and a server crashing.

# 

# \## Highlights

# 

# \- \*\*Geo-partitioned.\*\* The city is a grid of 0.1 degree zones, split between two shards (north and south of latitude 13.0). Each shard is a primary and a replica.

# \- \*\*No double booking.\*\* A rider first looks for candidates (no locks), then claims one with an atomic per-driver operation. A failed claim means someone else was faster, so the rider tries the next candidate.

# \- \*\*Hot spot resistant.\*\* Riders choose randomly among the 5 nearest free drivers, found with a size-5 heap, so a crowd doesn't all chase one driver.

# \- \*\*Fault tolerant.\*\* Each replica copies its primary every second. If a primary stops answering, the gateway switches the shard to the replica and promotes it.

# \- \*\*Event-driven.\*\* Every ride step is published to Kafka. A saga undoes a match automatically when payment fails.

# \- \*\*Idempotent.\*\* Repeating a ride request with the same `rideId` returns the same driver.

# \- \*\*Observable.\*\* A live dashboard shows server health, driver positions and the event stream.

# \- \*\*Tested for correctness.\*\* A load test fires hundreds of concurrent requests and checks that no driver was assigned twice.

# 

# \## Architecture

# 

# ```mermaid

# flowchart LR

# &#x20;   C\["Riders and drivers"] --> G\["Gateway :8080"]

# &#x20;   subgraph S\["South shard (latitude below 13.0)"]

# &#x20;       S1\["south-primary :8081"] -->|"copies every 1 s"| S2\["south-replica :8083"]

# &#x20;   end

# &#x20;   subgraph N\["North shard (latitude 13.0 and above)"]

# &#x20;       N1\["north-primary :8082"] -->|"copies every 1 s"| N2\["north-replica :8084"]

# &#x20;   end

# &#x20;   G --> S1

# &#x20;   G --> N1

# &#x20;   G -.->|"ride events"| K\[("Kafka: ride-events")]

# &#x20;   K --> R\["ride-service :8085<br/>trip tracker, payment, saga, dashboard"]

# &#x20;   R -->|"release driver"| G

# ```

# 

# \## Life of a ride request

# 

# ```mermaid

# sequenceDiagram

# &#x20;   participant R as Rider

# &#x20;   participant G as Gateway

# &#x20;   participant S as Zone servers

# &#x20;   participant K as Kafka

# &#x20;   participant P as Payment service

# &#x20;   participant C as Compensator

# &#x20;   R->>G: POST /rides/request (rideId)

# &#x20;   G->>K: RideRequested

# &#x20;   G->>S: look: nearest 5 free drivers from each shard

# &#x20;   S-->>G: candidates

# &#x20;   G->>S: claim a random one of the nearest 5

# &#x20;   S-->>G: claimed (or 409, so try the next one)

# &#x20;   G->>K: DriverMatched

# &#x20;   G-->>R: matched driver

# &#x20;   K->>P: DriverMatched

# &#x20;   alt payment succeeds

# &#x20;       P->>K: PaymentAuthorized

# &#x20;   else payment fails

# &#x20;       P->>K: PaymentFailed

# &#x20;       K->>C: PaymentFailed

# &#x20;       C->>G: release the driver

# &#x20;       C->>K: RideCancelled

# &#x20;   end

# ```

# 

# \## Components

# 

# | Component | Port | Role |

# |---|---|---|

# | `gateway` | 8080 | Entry point. Routes by latitude, runs the look-then-claim matching, tracks which shard each driver lives on, hands drivers over between shards, fails over to replicas, publishes ride events, and enforces idempotency. |

# | `dispatch-platform` (zone server) | 8081 to 8084 | Holds driver locations in memory, grouped by zone. Offers lookup, claim, remove and adopt operations. Run four times: two primaries and two replicas. |

# | `ride-service` | 8085 | Kafka consumers: trip tracker (history of each ride), simulated payment, and the saga compensator. Also serves the live dashboard. |

# | Kafka | 9092 | Topic `ride-events`, 3 partitions, keyed by `rideId` so each ride's events stay in order. |

# | `loadtest` | n/a | Concurrent load test that checks correctness and reports throughput and latency. |

# | `scripts` | n/a | Start, demo and stop scripts (Windows PowerShell). |

# 

# \## Design decisions and trade-offs

# 

# | Decision | Why | Trade-off |

# |---|---|---|

# | Shard by latitude, zones on a 0.1 degree grid | A simple rule any server can compute | Border riders must search across shards; busy zones are hot spots |

# | Look first, then claim | Lookups need no lock, only the claim is atomic | A claim can fail, so there is a retry loop (up to 5 rounds) |

# | Per-driver atomic claim (`ConcurrentHashMap.computeIfPresent`) instead of a zone-wide lock | Riders wanting different drivers never wait for each other | Protects one server only, so cross-server claims go through the owning server |

# | The server owns busy or free status | A stale location ping can never free a busy driver | Pings carry position only |

# | Gateway coordinates matching | One place for routing, retries and failover | The gateway is a single point of failure |

# | Replica pulls a snapshot every second (asynchronous) | Simple, and replica traffic never slows the primary | Replication lag: the last second of changes can be lost on failover |

# | Reactive failover with promotion | No extra heartbeat system needed | Slower detection; a wrong decision could cause split brain |

# | Random choice among the nearest 5 | Spreads crowds, far fewer collisions | A rider may get the 2nd to 5th nearest driver |

# | Kafka events keyed by `rideId` | Per-ride ordering and decoupled services | At-least-once delivery, so consumers must be idempotent |

# | Saga by choreography | Services only react to events, no central coordinator | Harder to follow as it grows; orchestration is the alternative |

# | Server-Sent Events for the dashboard feed | One-way push over plain HTTP | Not suitable if the browser needed to send data back |

# 

# \## Results

# 

# Measured with `java LoadTest.java 2000 1000 100` (2000 drivers, 1000 concurrent ride requests, 100 threads), Kafka events enabled, after warm-up:

# 

# | Metric | Result |

# |---|---|

# | Riders matched | 1000 of 1000 |

# | Double bookings | 0 |

# | Throughput | about 2,000 requests per second |

# | Latency p50 / p95 / p99 | 36 ms / 76 ms / 100 ms |

# 

# Machine: <your laptop's CPU and RAM>, with the gateway, four zone servers, ride-service, Kafka and the load generator all running on it. So the numbers measure the design, not production hardware. The first run after a restart is slower while the JVMs warm up.

# 

# The load test checks three things at once: no driver id is handed out twice, the number of matched riders equals the number of drivers the system reports as busy, and every response is accounted for.

# 

# \*\*Failure experiments\*\*

# \- Stopping a primary: the dashboard shows it as down, the replica is promoted, and its drivers (with busy or free status) stay on the map.

# \- Stopping a primary during heavy load: a few drivers can end up marked busy with no rider assigned (3 of 200 in one run), because of replication lag. See the limitations below.

# 

# \## Run it

# 

# Prerequisites: Java 17 or newer, Docker Desktop, and Windows PowerShell. Maven is included through the Maven wrapper.

# 

# ```powershell

# git clone https://github.com/Ganesh-Srinath/geo-partitioned-dispatch-platform.git

# cd geo-partitioned-dispatch-platform\\scripts

# powershell -ExecutionPolicy Bypass -File .\\start-all.ps1 -Build

# ```

# 

# The first run builds everything and pulls the Kafka image, which takes a few minutes. When it finishes, the dashboard opens at `http://localhost:8085/`. Then:

# 

# ```powershell

# powershell -ExecutionPolicy Bypass -File .\\demo.ps1     # drivers, rides, and one failed payment

# cd ..\\loadtest

# java LoadTest.java 2000 1000 100                         # run it a few times, use the later runs

# cd ..\\scripts

# powershell -ExecutionPolicy Bypass -File .\\stop-all.ps1

# ```

# 

# \*\*Manual start (any operating system):\*\* start Kafka with `docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0`. In each of the three Spring projects run `./mvnw clean package -DskipTests`. Then start the zone server jar four times with `--server.port=8081`, `--server.port=8082`, `--server.port=8083 --replica.of=http://localhost:8081` and `--server.port=8084 --replica.of=http://localhost:8082`. Finally start the `ride-service` jar and the `gateway` jar.

# 

# \## API

# 

# Gateway (port 8080):

# 

# | Endpoint | Purpose |

# |---|---|

# | `POST /drivers/location` | Report a driver's position: `{"id","lat","lng","free"}`. New drivers start free, and busy or free status is owned by the server. |

# | `POST /rides/request` | Request a ride: `{"riderId","rideId","lat","lng"}`. `rideId` is optional and acts as an idempotency key. Rider ids starting with `nopay` trigger a failed payment. |

# | `POST /rides/complete/{driverId}` | Finish a ride and free the driver. |

# | `GET /drivers` | List all drivers in the cluster. |

# 

# ride-service (port 8085): `GET /trips/{rideId}` returns a ride's event history, `GET /trips` returns counts by status, and `GET /` opens the dashboard.

# 

# \## Known limitations and what would come next

# 

# \- \*\*Replication is asynchronous.\*\* A claim made in the last second before a primary dies may be missing on the replica, which can allow a double assignment after failover. Fix: synchronous replication, or a replicated log with consensus such as Raft.

# \- \*\*The gateway is a single point of failure.\*\* Real deployments run several copies behind a load balancer, with the driver-to-shard map in a shared store.

# \- \*\*State is in memory.\*\* If both a primary and its replica die, that shard's drivers are lost. The gateway's idempotency map and the trip history are also in memory. Production would use a database or Redis.

# \- \*\*Driver hand-off between shards is two calls and not atomic.\*\* A gateway crash between them could lose a driver until its next location ping.

# \- \*\*Events can be lost in a crash between a state change and publishing it\*\* (the dual write problem). Fix: the transactional outbox pattern.

# \- \*\*Failure detection is reactive,\*\* and a slow server looks the same as a dead one.

# \- \*\*Events are hand-written JSON.\*\* A real system would use a schema registry (Avro or Protobuf).

# \- \*\*Distance is straight-line on latitude and longitude,\*\* not road distance, and there is no authentication.

# 

# \## Repository layout

# 

# ```

# dispatch-platform/   zone server (primary or replica)

# gateway/             gateway and coordinator

# ride-service/        Kafka consumers and dashboard

# loadtest/            LoadTest.java

# scripts/             start-all, demo, stop-all

# ```

