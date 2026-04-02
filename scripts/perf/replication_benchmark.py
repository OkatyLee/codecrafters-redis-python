from __future__ import annotations

import argparse
import asyncio
import time

from redis_protocol import RESPErrorReply, encode_command, percentile, read_response


async def send_command(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *parts: bytes | str | int,
):
    writer.write(encode_command(*parts))
    await writer.drain()
    return await read_response(reader)


async def wait_for_replication_ready(
    master_host: str,
    master_port: int,
    replica_host: str,
    replica_port: int,
    timeout: float,
) -> None:
    master_reader, master_writer = await asyncio.open_connection(master_host, master_port)
    replica_reader, replica_writer = await asyncio.open_connection(replica_host, replica_port)

    try:
        deadline = asyncio.get_running_loop().time() + timeout
        seed_key = b"perf:replication:seed"
        seed_value = f"ready:{time.time_ns()}".encode("ascii")
        while asyncio.get_running_loop().time() < deadline:
            response = await send_command(master_reader, master_writer, b"SET", seed_key, seed_value)
            if response != b"OK":
                raise RuntimeError(f"Unexpected SET response: {response!r}")

            replica_value = await send_command(replica_reader, replica_writer, b"GET", seed_key)
            if replica_value == seed_value:
                return
            await asyncio.sleep(0.05)
        raise TimeoutError("Replica did not catch up during readiness check")
    finally:
        master_writer.close()
        replica_writer.close()
        await master_writer.wait_closed()
        await replica_writer.wait_closed()


async def worker(
    worker_id: int,
    queue: asyncio.Queue[int],
    *,
    master_host: str,
    master_port: int,
    replica_host: str,
    replica_port: int,
    payload_size: int,
    verify_timeout: float,
    write_latencies: list[float],
    replication_latencies: list[float],
    failures: list[str],
) -> None:
    master_reader, master_writer = await asyncio.open_connection(master_host, master_port)
    replica_reader, replica_writer = await asyncio.open_connection(replica_host, replica_port)

    try:
        while True:
            try:
                message_index = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            key = f"perf:replication:{worker_id}:{message_index}".encode("ascii")
            value_prefix = f"{worker_id}:{message_index}:".encode("ascii")
            filler = b"x" * max(1, payload_size - len(value_prefix))
            value = value_prefix + filler
            started_at = time.perf_counter()

            try:
                response = await send_command(master_reader, master_writer, b"SET", key, value)
                if response != b"OK":
                    failures.append(f"Unexpected SET response for {key!r}: {response!r}")
                    queue.task_done()
                    continue
            except RESPErrorReply as exc:
                failures.append(f"SET failed for {key!r}: {exc}")
                queue.task_done()
                continue

            write_done_at = time.perf_counter()
            write_latencies.append(write_done_at - started_at)

            deadline = asyncio.get_running_loop().time() + verify_timeout
            replicated = False
            while asyncio.get_running_loop().time() < deadline:
                replica_value = await send_command(replica_reader, replica_writer, b"GET", key)
                if replica_value == value:
                    replication_latencies.append(time.perf_counter() - started_at)
                    replicated = True
                    break
                await asyncio.sleep(0.005)

            if not replicated:
                failures.append(f"Replica did not observe {key.decode('ascii')} within {verify_timeout:.2f}s")

            queue.task_done()
    finally:
        master_writer.close()
        replica_writer.close()
        await master_writer.wait_closed()
        await replica_writer.wait_closed()


async def main_async(args: argparse.Namespace) -> int:
    await wait_for_replication_ready(
        args.master_host,
        args.master_port,
        args.replica_host,
        args.replica_port,
        args.readiness_timeout,
    )

    queue: asyncio.Queue[int] = asyncio.Queue()
    for idx in range(args.messages):
        queue.put_nowait(idx)

    write_latencies: list[float] = []
    replication_latencies: list[float] = []
    failures: list[str] = []

    started_at = time.perf_counter()
    workers = [
        asyncio.create_task(
            worker(
                worker_id,
                queue,
                master_host=args.master_host,
                master_port=args.master_port,
                replica_host=args.replica_host,
                replica_port=args.replica_port,
                payload_size=args.payload_size,
                verify_timeout=args.verify_timeout,
                write_latencies=write_latencies,
                replication_latencies=replication_latencies,
                failures=failures,
            )
        )
        for worker_id in range(args.concurrency)
    ]

    await queue.join()
    for task in workers:
        await task
    elapsed = time.perf_counter() - started_at

    successful_replications = len(replication_latencies)
    print(f"replication writes: {args.messages}")
    print(f"successful replications: {successful_replications}")
    print(f"failed replications: {len(failures)}")
    print(f"total duration seconds: {elapsed:.4f}")
    print(f"write throughput ops/sec: {args.messages / elapsed:.2f}")
    print(f"write latency p50 ms: {percentile(write_latencies, 0.50) * 1000:.3f}")
    print(f"write latency p95 ms: {percentile(write_latencies, 0.95) * 1000:.3f}")
    print(f"replication visibility p50 ms: {percentile(replication_latencies, 0.50) * 1000:.3f}")
    print(f"replication visibility p95 ms: {percentile(replication_latencies, 0.95) * 1000:.3f}")
    print(f"replication visibility max ms: {max(replication_latencies, default=0.0) * 1000:.3f}")

    if failures:
        print("replication failures:")
        for failure in failures[:10]:
            print(f"- {failure}")
        if len(failures) > 10:
            print(f"- ... and {len(failures) - 10} more")
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark master-replica propagation")
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=6379)
    parser.add_argument("--replica-host", default="127.0.0.1")
    parser.add_argument("--replica-port", type=int, default=6380)
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--verify-timeout", type=float, default=2.0)
    parser.add_argument("--readiness-timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
