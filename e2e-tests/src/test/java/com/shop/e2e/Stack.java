package com.shop.e2e;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The whole system, started once per JVM: infrastructure from {@code infra/docker-compose.yml}
 * (five PostgreSQL, Kafka, Debezium Connect), the five services as the jars Maven built, and the four
 * outbox connectors (Customer publishes nothing). Uses the fixed host ports of the compose file, so the development stack must be down.
 */
public final class Stack {

    public static final String PRODUCT = "http://localhost:8081";
    public static final String ORDER = "http://localhost:8080";
    public static final String INVENTORY = "http://localhost:8082";
    public static final String PAYMENT = "http://localhost:8084";
    public static final String CUSTOMER = "http://localhost:8085";
    public static final String CONNECT = "http://localhost:8083";
    public static final String KAFKA = "localhost:29092";

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path COMPOSE = ROOT.resolve("infra/docker-compose.yml");
    private static final Path LOGS = Path.of("target/e2e-logs").toAbsolutePath();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static final Map<String, Process> PROCESSES = new LinkedHashMap<>();
    private static boolean started;

    private Stack() {
    }

    /** Idempotent: the first test class starts everything, the others reuse it. */
    public static synchronized void start() {
        if (started) {
            return;
        }
        try {
            Files.createDirectories(LOGS);
            Runtime.getRuntime().addShutdownHook(new Thread(Stack::stop, "e2e-stack-stop"));
            compose("up", "-d", "--wait", "product-db", "order-db", "inventory-db", "payment-db", "customer-db",
                    "kafka", "connect");
            compose("up", "kafka-init");
            startService("product");
            startService("inventory");
            startService("payment");
            startService("customer");
            startService("order");
            for (String name : List.of("product", "order", "inventory", "payment")) {
                registerConnector(name);
            }
            started = true;
        } catch (Exception e) {
            stop();
            throw new IllegalStateException("Could not start the end-to-end stack: " + e.getMessage(), e);
        }
    }

    /** Stops one service (product, inventory, payment, customer or order), as if it crashed. */
    public static synchronized void stopService(String name) {
        Process p = PROCESSES.remove(name);
        if (p != null) {
            p.destroy();
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Starts a service again after {@link #stopService}; does nothing if it is running. */
    public static synchronized void restartService(String name) {
        Process running = PROCESSES.get(name);
        if (running != null && running.isAlive()) {
            return;
        }
        try {
            startService(name);
        } catch (Exception e) {
            throw new IllegalStateException("Could not restart " + name + "-service: " + e.getMessage(), e);
        }
    }

    private static synchronized void stop() {
        PROCESSES.values().forEach(Process::destroy);
        PROCESSES.clear();
        if (!Boolean.getBoolean("e2e.keep")) {
            try {
                compose("down", "-v");
            } catch (Exception ignored) {
                // best effort at shutdown
            }
        }
    }

    // ---- processes -------------------------------------------------------------------------------------

    private static void compose(String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", COMPOSE.toString()));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd)
                .directory(COMPOSE.getParent().toFile())
                .redirectErrorStream(true)
                .redirectOutput(LOGS.resolve("compose.log").toFile())
                .start();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("docker compose " + String.join(" ", args) + " failed, see " + LOGS.resolve("compose.log"));
        }
    }

    private static final Map<String, Integer> PORTS =
            Map.of("order", 8080, "product", 8081, "inventory", 8082, "payment", 8084, "customer", 8085);

    // short saga timeout so the timeout scenarios do not take minutes; long enough for a cold start
    private static final Map<String, Map<String, String>> ENVIRONMENT =
            Map.of("order", Map.of("SHOP_ORDER_SAGA_TIMEOUT", "20s", "SHOP_ORDER_SWEEP_INTERVAL", "1s"));

    private static void startService(String name) throws Exception {
        int port = PORTS.get(name);
        Map<String, String> env = ENVIRONMENT.getOrDefault(name, Map.of());
        File jar = ROOT.resolve(name + "-service/target/" + name + "-service-1.0.0-SNAPSHOT.jar").toFile();
        if (!jar.isFile()) {
            throw new IllegalStateException(jar + " not found: run `mvn -DskipTests package` in " + name + "-service first");
        }
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jar.getPath())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(LOGS.resolve(name + ".log").toFile()));
        pb.environment().putAll(env);
        PROCESSES.put(name, pb.start());
        awaitHealthy(name, port);
    }

    private static void awaitHealthy(String name, int port) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline) {
            if (!PROCESSES.get(name).isAlive()) {
                throw new IllegalStateException(name + "-service exited, see " + LOGS.resolve(name + ".log"));
            }
            try {
                HttpResponse<String> r = get("http://localhost:" + port + "/actuator/health");
                if (r.statusCode() == 200 && r.body().contains("\"UP\"")) {
                    return;
                }
            } catch (IOException notYet) {
                // still starting
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(name + "-service not healthy after 120s, see " + LOGS.resolve(name + ".log"));
    }

    // ---- Debezium --------------------------------------------------------------------------------------

    private static void registerConnector(String name) throws Exception {
        String connector = name + "-outbox";
        String body = Files.readString(ROOT.resolve("infra/debezium/" + connector + ".json"));
        HttpResponse<String> put = HTTP.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors/" + connector + "/config"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        if (put.statusCode() / 100 != 2) {
            throw new IllegalStateException("Registering " + connector + " failed: " + put.statusCode() + " " + put.body());
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            HttpResponse<String> status = get(CONNECT + "/connectors/" + connector + "/status");
            if (status.statusCode() == 200) {
                JsonNode node = JSON.readTree(status.body());
                boolean connectorUp = "RUNNING".equals(node.path("connector").path("state").asString());
                boolean taskUp = node.path("tasks").size() > 0 && "RUNNING".equals(node.path("tasks").get(0).path("state").asString());
                if (connectorUp && taskUp) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(connector + " is not RUNNING after 90s: " + get(CONNECT + "/connectors/" + connector + "/status").body());
    }

    private static HttpResponse<String> get(String url) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
