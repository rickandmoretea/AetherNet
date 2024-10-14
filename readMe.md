## Aether - Real time Scalable AS Traffic Analyzer

Aether is a comprehensive live network traffic analyzer designed to enrich and visualize network flow data.. The tool integrates with Kafka for data streaming, TimescaleDB for time-series data storage, Prometheus for metrics collection, and Grafana for data visualization.

### Features 

- **Real-time Data Streaming**: Aether uses Kafka for real-time data streaming. It can handle large volumes of data and scale horizontally.
- **Time-series Data Storage**: Aether uses TimescaleDB for storing processed data for querying and visualization.
- **Metrics Collection**: Aether uses Prometheus for collecting metrics from the network devices.
- **Data Visualization**: Aether uses Grafana for visualizing the network flow data.

### Setup

1. Clone the repository
```chatinput
git clone <respository-url>
cd aether
```
2. Build and run the docker containers
```chatinput
docker-compose up -d
```
3. Install Rust dependencies
4. Install Node dependencies

Running the Application 
1. Start the NetFlow Processor
```rust
cargo run --bin netflow_processor
```
2. Access the API server will be runnnign at `http://localhost:8000`
3. Access the Grafana dashboard at `http://localhost:3000` with the credentials `admin:admin`

### API Endpoints
- /top_countries : Get the top countries by traffic
- /top_as : Retrieves top AS by traffic
- /traffic_by_country: Retrieves traffic by country
- /traffic_by_as: Retrieves traffic by AS


