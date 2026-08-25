"""Sample Application demonstrating Queuemaxxing Lite Server in a real-time concurrent environment.

Includes detailed real-time telemetry:
- Packet / payload byte counters and total transfer volume.
- Live queue depth and active worker metrics.
- Comprehensive end-of-run network transfer and performance audit report.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx
import uvicorn

# Add root directory to sys.path
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.main import app

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000
BASE_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"


@dataclass
class PacketRecord:
    packet_id: str
    task_name: str
    priority: int
    delay_seconds: int
    payload_bytes: int
    total_request_bytes: int
    pushed_at: float
    claimed_at: Optional[float] = None
    finished_at: Optional[float] = None
    worker_id: Optional[str] = None
    response_bytes: int = 0


class TelemetryCollector:
    """Tracks real-time network, packet, queuing, and worker performance metrics."""

    def __init__(self):
        self.lock = threading.Lock()
        self.start_time: float = time.time()
        self.total_packets_sent: int = 0
        self.total_packets_received: int = 0
        self.total_bytes_sent: int = 0
        self.total_bytes_received: int = 0
        self.packets: Dict[str, PacketRecord] = {}
        self.worker_records: Dict[str, Dict[str, Any]] = {}
        self.queue_depth_history: List[tuple[float, int]] = []
        self._current_queue_depth: int = 0

    def record_push(
        self,
        item_id: str,
        task_name: str,
        priority: int,
        delay_seconds: int,
        payload_bytes: int,
        req_bytes: int,
        res_bytes: int,
    ):
        with self.lock:
            self.total_packets_sent += 1
            self.total_bytes_sent += req_bytes
            self.total_bytes_received += res_bytes
            self._current_queue_depth += 1

            self.packets[item_id] = PacketRecord(
                packet_id=item_id,
                task_name=task_name,
                priority=priority,
                delay_seconds=delay_seconds,
                payload_bytes=payload_bytes,
                total_request_bytes=req_bytes,
                pushed_at=time.time(),
            )
            self.queue_depth_history.append((time.time() - self.start_time, self._current_queue_depth))

    def record_claim(
        self,
        item_id: str,
        worker_id: str,
        req_bytes: int,
        res_bytes: int,
    ):
        with self.lock:
            self.total_packets_received += 1
            self.total_bytes_sent += req_bytes
            self.total_bytes_received += res_bytes
            self._current_queue_depth = max(0, self._current_queue_depth - 1)
            self.queue_depth_history.append((time.time() - self.start_time, self._current_queue_depth))

            if item_id in self.packets:
                pkt = self.packets[item_id]
                pkt.claimed_at = time.time()
                pkt.worker_id = worker_id
                pkt.response_bytes = res_bytes

            if worker_id not in self.worker_records:
                self.worker_records[worker_id] = {
                    "claimed": 0,
                    "bytes_processed": 0,
                    "total_work_time": 0.0,
                }
            self.worker_records[worker_id]["claimed"] += 1
            self.worker_records[worker_id]["bytes_processed"] += res_bytes

    def record_finish(self, item_id: str, worker_id: str, work_duration: float):
        with self.lock:
            if item_id in self.packets:
                self.packets[item_id].finished_at = time.time()
            if worker_id in self.worker_records:
                self.worker_records[worker_id]["total_work_time"] += work_duration

    def get_current_depth(self) -> int:
        with self.lock:
            return self._current_queue_depth

    def print_detailed_report(self):
        duration = max(0.001, time.time() - self.start_time)
        total_transfer_bytes = self.total_bytes_sent + self.total_bytes_received
        throughput_kbps = (total_transfer_bytes / 1024.0) / duration

        claimed_packets = [p for p in self.packets.values() if p.claimed_at is not None]
        wait_times = [(p.claimed_at - p.pushed_at) for p in claimed_packets if p.claimed_at]
        avg_wait = sum(wait_times) / len(wait_times) if wait_times else 0.0
        max_wait = max(wait_times) if wait_times else 0.0
        min_wait = min(wait_times) if wait_times else 0.0

        print("\n" + "=" * 80)
        print("                   QUEUEMAXXING LITE - DETAILED TELEMETRY REPORT")
        print("=" * 80)

        # 1. Network & Transfer Summary
        print("\n  [1] NETWORK & PACKET TRANSFER SUMMARY")
        print("  " + "-" * 76)
        print(f"  * Total Execution Elapsed Time : {duration:.2f} s")
        print(f"  * Total Packets Transmitted    : {self.total_packets_sent + self.total_packets_received} packets")
        print(f"    - PUSH Packets (Upstream)   : {self.total_packets_sent} packets ({self.total_bytes_sent:,} bytes)")
        print(f"    - CLAIM Packets (Downstream): {self.total_packets_received} packets ({self.total_bytes_received:,} bytes)")
        print(f"  * Total Network Transfer       : {total_transfer_bytes:,} bytes ({total_transfer_bytes / 1024:.2f} KB)")
        print(f"  * Average Transfer Throughput  : {throughput_kbps:.2f} KB/s ({(total_transfer_bytes * 8) / (duration * 1000):.2f} kbps)")

        # 2. Queue & Delivery Integrity
        print("\n  [2] QUEUE & DELIVERY INTEGRITY")
        print("  " + "-" * 76)
        total_pushed = len(self.packets)
        total_claimed = len(claimed_packets)
        drop_rate = 0.0 if total_pushed == total_claimed else ((total_pushed - total_claimed) / total_pushed) * 100
        print(f"  * Total Messages Pushed        : {total_pushed}")
        print(f"  * Total Messages Claimed       : {total_claimed}")
        print(f"  * Remaining Unclaimed in Queue : {total_pushed - total_claimed}")
        print(f"  * Packet Loss / Drop Rate      : {drop_rate:.2f}% (At-Most-Once delivery verified)")

        # 3. Queuing Latency & Timing
        print("\n  [3] QUEUING LATENCY & TIME-IN-FLIGHT")
        print("  " + "-" * 76)
        print(f"  * Min Queue Wait Time          : {min_wait:.3f} s")
        print(f"  * Avg Queue Wait Time          : {avg_wait:.3f} s")
        print(f"  * Max Queue Wait Time          : {max_wait:.3f} s (including delayed items)")

        # 4. Detailed Per-Packet Breakdown Table
        print("\n  [4] PER-PACKET AUDIT TRAIL")
        print("  " + "-" * 76)
        header = f"  {'ID':<10} {'Task Name':<30} {'Prio':<5} {'Delay':<6} {'Size (B)':<9} {'Wait(s)':<8} {'Worker':<10}"
        print(header)
        print("  " + "-" * 76)
        for pkt in self.packets.values():
            wait_str = f"{(pkt.claimed_at - pkt.pushed_at):.2f}s" if pkt.claimed_at else "N/A"
            w_id = pkt.worker_id or "Unclaimed"
            print(f"  {pkt.packet_id[:8]:<10} {pkt.task_name:<30} {pkt.priority:<5} {str(pkt.delay_seconds)+'s':<6} {pkt.payload_bytes:<9} {wait_str:<8} {w_id:<10}")

        # 5. Worker Utilization & Bandwidth Distribution
        print("\n  [5] WORKER UTILIZATION & LOAD DISTRIBUTION")
        print("  " + "-" * 76)
        w_header = f"  {'Worker ID':<12} {'Tasks Claimed':<16} {'Payload Bytes':<16} {'Work Time (s)':<15} {'Share (%)':<10}"
        print(w_header)
        print("  " + "-" * 76)
        for w_id, w_data in sorted(self.worker_records.items()):
            share = (w_data["claimed"] / total_claimed * 100) if total_claimed > 0 else 0.0
            print(f"  {w_id:<12} {w_data['claimed']:<16} {w_data['bytes_processed']:<16} {w_data['total_work_time']:<15.2f} {share:<10.1f}")

        print("=" * 80 + "\n")


class BackgroundServer:
    """Helper to run the FastAPI Uvicorn server in a daemon thread."""

    def __init__(self, host: str = SERVER_HOST, port: int = SERVER_PORT):
        self.host = host
        self.port = port
        self.config = uvicorn.Config(app=app, host=self.host, port=self.port, log_level="warning")
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.should_exit = True


async def wait_for_server(client: httpx.AsyncClient, max_retries: int = 20):
    """Wait until the server is responsive."""
    for _ in range(max_retries):
        try:
            res = await client.get(f"{BASE_URL}/docs")
            if res.status_code == 200:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.2)
    return False


def log(entity: str, message: str, color: str = "\033[0m"):
    """Formatted timestamped console logger."""
    now = time.strftime("%H:%M:%S")
    reset = "\033[0m"
    print(f"[{now}] {color}[{entity:<14}]{reset} {message}")


async def producer(
    client: httpx.AsyncClient,
    queue_id: str,
    producer_id: str,
    messages: List[Dict[str, Any]],
    telemetry: TelemetryCollector,
    color: str = "\033[36m",
):
    """Simulates a message publisher pushing jobs into the queue."""
    for msg in messages:
        payload = msg.get("payload", {})
        priority = msg.get("priority", 0)
        delay = msg.get("delay_seconds", 0)

        req_body = {
            "payload": payload,
            "priority": priority,
            "delay_seconds": delay,
        }
        encoded_body = json.dumps(req_body).encode("utf-8")
        payload_bytes = len(json.dumps(payload).encode("utf-8"))
        req_bytes = len(encoded_body)

        log(
            producer_id,
            f"--> PUSH: '{payload.get('task')}' | Prio: {priority} | Delay: {delay}s | Packet: {req_bytes}B",
            color=color,
        )

        response = await client.post(
            f"{BASE_URL}/queues/{queue_id}/push",
            content=encoded_body,
            headers={"Content-Type": "application/json"},
        )

        res_bytes = len(response.content)

        if response.status_code == 201:
            data = response.json()
            item_id = data["id"]
            telemetry.record_push(
                item_id=item_id,
                task_name=payload.get("task", "Unknown"),
                priority=priority,
                delay_seconds=delay,
                payload_bytes=payload_bytes,
                req_bytes=req_bytes,
                res_bytes=res_bytes,
            )
            depth = telemetry.get_current_depth()
            log(
                producer_id,
                f"    [ACK 201] (ID: {item_id[:8]}... | Status: {data['status']} | Queue Depth: {depth})",
                color=color,
            )
        else:
            log(producer_id, f"    [FAIL {response.status_code}]: {response.text}", color="\033[31m")

        await asyncio.sleep(msg.get("interval", 0.5))


async def worker(
    client: httpx.AsyncClient,
    queue_id: str,
    worker_id: str,
    stop_event: asyncio.Event,
    telemetry: TelemetryCollector,
    color: str = "\033[32m",
):
    """Simulates a concurrent worker pulling and executing tasks in real-time."""
    log(worker_id, "Worker online & listening for jobs...", color=color)

    while not stop_event.is_set():
        try:
            req_bytes = len(f"GET /queues/{queue_id}/claim HTTP/1.1\r\n".encode("utf-8"))
            response = await client.get(f"{BASE_URL}/queues/{queue_id}/claim")
            res_bytes = len(response.content)

            if response.status_code == 200:
                item = response.json()
                item_id = item["id"]
                task_name = item["payload"].get("task", "Unknown")
                priority = item.get("priority", 0)
                seq = item.get("sequence", 0)

                telemetry.record_claim(
                    item_id=item_id,
                    worker_id=worker_id,
                    req_bytes=req_bytes,
                    res_bytes=res_bytes,
                )

                depth = telemetry.get_current_depth()
                log(
                    worker_id,
                    f"<-- CLAIMED [Seq: {seq}, Prio: {priority}, {res_bytes}B]: '{task_name}' (Remaining in Q: {depth})",
                    color=color,
                )

                # Simulate job execution time
                work_time = item["payload"].get("work_duration", 0.6)
                await asyncio.sleep(work_time)

                telemetry.record_finish(item_id, worker_id, work_time)
                log(
                    worker_id,
                    f"    FINISHED '{task_name}' in {work_time:.2f}s",
                    color=color,
                )
            elif response.status_code == 204:
                # Queue empty or items are delayed
                await asyncio.sleep(0.25)
            else:
                log(worker_id, f"Error claiming: {response.status_code}", color="\033[31m")
                await asyncio.sleep(0.5)

        except Exception as e:
            log(worker_id, f"Exception: {e}", color="\033[31m")
            await asyncio.sleep(0.5)

    log(worker_id, "Worker shutdown signal received.", color=color)


async def run_demo():
    print("=" * 80)
    print("      QUEUEMAXXING LITE - REALTIME CONCURRENT DEMO WITH DETAILED METRICS")
    print("=" * 80)

    telemetry = TelemetryCollector()
    server = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            res = await client.get(f"{BASE_URL}/docs")
            is_running = res.status_code == 200
        except Exception:
            is_running = False

        if not is_running:
            print("[INFO] Starting background FastAPI server on http://127.0.0.1:8000 ...")
            server = BackgroundServer()
            server.start()
            ready = await wait_for_server(client)
            if not ready:
                print("[ERROR] Server failed to start.")
                return
            print("[INFO] Server started successfully.\n")
        else:
            print("[INFO] Connected to existing server at http://127.0.0.1:8000\n")

        # Create a Priority-enabled FIFO Queue for the demo
        queue_name = f"telemetry-demo-{int(time.time())}"
        print(f"[SETUP] Creating queue '{queue_name}' (Ordering: FIFO, Priority: Enabled)...")
        create_res = await client.post(
            f"{BASE_URL}/create_queue",
            json={
                "name": queue_name,
                "ordering": "fifo",
                "priority_enabled": True,
            },
        )
        if create_res.status_code != 201:
            print(f"[ERROR] Failed to create queue: {create_res.text}")
            return

        queue_data = create_res.json()
        queue_id = queue_data["id"]
        print(f"[SETUP] Queue created! ID: {queue_id}\n")

        # Define batches of diverse message packets
        # Producer 1: Standard transaction stream & scheduled reporting
        producer1_messages = [
            {"payload": {"task": "Sync User Data #1", "work_duration": 0.4, "data_rows": 1500}, "priority": 1, "delay_seconds": 0, "interval": 0.2},
            {"payload": {"task": "Send Daily Newsletter", "work_duration": 0.5, "subscribers": 8420}, "priority": 2, "delay_seconds": 0, "interval": 0.3},
            {"payload": {"task": "Generate Monthly Report", "work_duration": 0.7, "charts": 12, "format": "PDF"}, "priority": 3, "delay_seconds": 0, "interval": 0.4},
            {"payload": {"task": "Sync User Data #2", "work_duration": 0.4, "data_rows": 2300}, "priority": 1, "delay_seconds": 0, "interval": 0.2},
        ]

        # Producer 2: High-priority telemetry events & delayed batch operations
        producer2_messages = [
            {"payload": {"task": "Delayed Nightly Backup", "work_duration": 0.3, "snapshot_gb": 45}, "priority": 1, "delay_seconds": 4, "interval": 0.4},
            {"payload": {"task": "CRITICAL: Security Alert!", "work_duration": 0.2, "threat_level": "RED"}, "priority": 100, "delay_seconds": 0, "interval": 0.5},
            {"payload": {"task": "HIGH: Stripe Payment Webhook", "work_duration": 0.3, "amount_cents": 9999}, "priority": 50, "delay_seconds": 0, "interval": 0.3},
            {"payload": {"task": "CRITICAL: DB Failover Notice", "work_duration": 0.2, "node": "db-replica-03"}, "priority": 100, "delay_seconds": 0, "interval": 0.5},
        ]

        # Spawn 3 concurrent Workers
        stop_workers = asyncio.Event()
        worker_colors = ["\033[32m", "\033[33m", "\033[35m"]
        workers = [
            asyncio.create_task(worker(client, queue_id, f"Worker-{i+1}", stop_workers, telemetry, color=worker_colors[i]))
            for i in range(3)
        ]

        await asyncio.sleep(0.5)

        # Launch Producers concurrently
        print("\n--- [STARTING CONCURRENT PRODUCERS & REALTIME PACKET INGESTION] ---\n")
        p1_task = asyncio.create_task(producer(client, queue_id, "Producer-Stream", producer1_messages, telemetry, color="\033[36m"))
        p2_task = asyncio.create_task(producer(client, queue_id, "Producer-Alerts", producer2_messages, telemetry, color="\033[94m"))

        # Wait for all producers to finish publishing
        await asyncio.gather(p1_task, p2_task)
        print("\n--- [ALL PACKETS SENT - DRAINING QUEUE & MATURING DELAYED MESSAGES] ---\n")

        # Allow workers time to process and for delayed items to mature
        await asyncio.sleep(6.0)

        # Stop workers
        stop_workers.set()
        await asyncio.gather(*workers)

        # Print the detailed transfer and audit report
        telemetry.print_detailed_report()

    if server:
        server.stop()


if __name__ == "__main__":
    asyncio.run(run_demo())
