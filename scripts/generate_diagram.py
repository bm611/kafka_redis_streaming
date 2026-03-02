from diagrams import Cluster, Diagram, Edge
from diagrams.generic.database import SQL
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.network import Zookeeper
from diagrams.onprem.queue import Kafka
from diagrams.programming.language import Python

graph_attr = {
    "fontsize": "28",
    "bgcolor": "white",
    "pad": "0.6",
    "ranksep": "1.0",
    "nodesep": "0.6",
    "splines": "ortho",
}

edge_attr = {
    "fontsize": "10",
}

with Diagram(
    "Real-Time Hotel Booking Events Pipeline",
    filename="docs/architecture",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    edge_attr=edge_attr,
    outformat="png",
):
    with Cluster("Ingestion"):
        producer = Python("Producer\n(Faker + Pydantic)")

    with Cluster("Kafka", graph_attr={"style": "rounded", "bgcolor": "#e8f4fd"}):
        zk = Zookeeper("Zookeeper")
        topic = Kafka("booking-events")
        zk - Edge(style="dashed", label="manages") - topic

    with Cluster("Consumer Groups", graph_attr={"style": "rounded", "bgcolor": "#f3e8fd"}):
        warehouse = Python("Warehouse\nService")
        analytics = Python("Analytics\nService")
        fraud = Python("Fraud Detection\nService")
        notifications = Python("Notifications\nService")

    with Cluster("Storage", graph_attr={"style": "rounded", "bgcolor": "#fdf8e8"}):
        sqlite = SQL("SQLite")
        redis = Redis("Redis")
        postgres = PostgreSQL("PostgreSQL\n(provisioned)")

    with Cluster("Dead Letter Queue", graph_attr={"style": "rounded", "bgcolor": "#fde8e8"}):
        dlq = Kafka("booking-events-dlq")

    with Cluster("Downstream"):
        query = Python("query_warehouse.py")

    # Producer -> Kafka
    producer >> Edge(label="publish", color="darkgreen") >> topic

    # Kafka -> Consumers
    topic >> Edge(color="#7b1fa2") >> warehouse
    topic >> Edge(color="#7b1fa2") >> analytics
    topic >> Edge(color="#7b1fa2") >> fraud
    topic >> Edge(color="#7b1fa2") >> notifications

    # Consumers -> Storage
    warehouse >> Edge(label="batch flush / 10 events", color="steelblue") >> sqlite
    analytics >> Edge(label="revenue counters", color="darkorange") >> redis
    fraud >> Edge(label="velocity (1h) + scores (24h)", color="darkorange") >> redis
    notifications >> Edge(label="dedup keys (TTL 7d)", color="darkorange") >> redis

    # Consumers -> DLQ (single grouped edge for clarity)
    [warehouse, analytics, fraud, notifications] >> Edge(
        style="dashed", color="red", label="on failure"
    ) >> dlq

    # Query script
    sqlite >> Edge(label="queried by", color="steelblue") >> query
