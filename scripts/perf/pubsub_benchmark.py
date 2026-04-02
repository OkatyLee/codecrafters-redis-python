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


async def subscriber(
    subscriber_id: int,
    *,
    host: str,
    port: int,
    channel: bytes,
    expected_messages: int,
    latencies: list[float],
    failures: list[str],
    ready_event: asyncio.Event,
) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        response = await send_command(reader, writer, b"SUBSCRIBE", channel)
        if response != [b"subscribe", channel, 1]:
            failures.append(f"subscriber {subscriber_id} received unexpected subscribe ack: {response!r}")
            ready_event.set()
            return

        ready_event.set()
        received = 0
        while received < expected_messages:
            message = await read_response(reader)
            if not isinstance(message, list) or len(message) != 3 or message[0] != b"message":
                failures.append(f"subscriber {subscriber_id} received unexpected payload: {message!r}")
                return

            payload = message[2]
            if not isinstance(payload, bytes):
                failures.append(f"subscriber {subscriber_id} received non-bytes payload: {payload!r}")
                return

            try:
                _, sent_ns, _ = payload.split(b":", 2)
                latencies.append((time.time_ns() - int(sent_ns)) / 1_000_000_000)
            except ValueError:
                failures.append(f"subscriber {subscriber_id} could not parse payload: {payload!r}")
                return
            received += 1
    finally:
        writer.close()
        await writer.wait_closed()


async def publisher(
    publisher_id: int,
    *,
    host: str,
    port: int,
    channel: bytes,
    start_index: int,
    message_count: int,
    subscriber_count: int,
    payload_size: int,
    publish_latencies: list[float],
    failures: list[str],
) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        for local_index in range(message_count):
            message_index = start_index + local_index
            payload_prefix = f"{publisher_id}-{message_index}:{time.time_ns()}:".encode("ascii")
            filler = b"x" * max(1, payload_size - len(payload_prefix))
            payload = payload_prefix + filler
            started_at = time.perf_counter()
            try:
                recipient_count = await send_command(reader, writer, b"PUBLISH", channel, payload)
            except RESPErrorReply as exc:
                failures.append(f"publisher {publisher_id} failed to publish message {message_index}: {exc}")
                continue
            publish_latencies.append(time.perf_counter() - started_at)
            if recipient_count != subscriber_count:
                failures.append(
                    f"publisher {publisher_id} expected {subscriber_count} recipients for message {message_index}, got {recipient_count!r}"
                )
    finally:
        writer.close()
        await writer.wait_closed()


async def main_async(args: argparse.Namespace) -> int:
    channel = args.channel.encode("ascii")
    delivery_latencies: list[float] = []
    publish_latencies: list[float] = []
    failures: list[str] = []
    ready_events = [asyncio.Event() for _ in range(args.subscribers)]

    subscriber_tasks = [
        asyncio.create_task(
            subscriber(
                idx,
                host=args.host,
                port=args.port,
                channel=channel,
                expected_messages=args.messages,
                latencies=delivery_latencies,
                failures=failures,
                ready_event=ready_events[idx],
            )
        )
        for idx in range(args.subscribers)
    ]

    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in ready_events)),
        timeout=args.subscription_timeout,
    )

    base, extra = divmod(args.messages, args.publishers)
    publish_tasks = []
    next_index = 0
    started_at = time.perf_counter()
    for publisher_id in range(args.publishers):
        message_count = base + (1 if publisher_id < extra else 0)
        publish_tasks.append(
            asyncio.create_task(
                publisher(
                    publisher_id,
                    host=args.host,
                    port=args.port,
                    channel=channel,
                    start_index=next_index,
                    message_count=message_count,
                    subscriber_count=args.subscribers,
                    payload_size=args.payload_size,
                    publish_latencies=publish_latencies,
                    failures=failures,
                )
            )
        )
        next_index += message_count

    await asyncio.gather(*publish_tasks)
    await asyncio.wait_for(asyncio.gather(*subscriber_tasks), timeout=args.delivery_timeout)
    elapsed = time.perf_counter() - started_at

    expected_deliveries = args.subscribers * args.messages
    print(f"published messages: {args.messages}")
    print(f"subscribers: {args.subscribers}")
    print(f"publishers: {args.publishers}")
    print(f"expected deliveries: {expected_deliveries}")
    print(f"recorded deliveries: {len(delivery_latencies)}")
    print(f"publish duration seconds: {elapsed:.4f}")
    print(f"publish throughput msgs/sec: {args.messages / elapsed:.2f}")
    print(f"publish latency p50 ms: {percentile(publish_latencies, 0.50) * 1000:.3f}")
    print(f"publish latency p95 ms: {percentile(publish_latencies, 0.95) * 1000:.3f}")
    print(f"delivery latency p50 ms: {percentile(delivery_latencies, 0.50) * 1000:.3f}")
    print(f"delivery latency p95 ms: {percentile(delivery_latencies, 0.95) * 1000:.3f}")
    print(f"delivery latency max ms: {max(delivery_latencies, default=0.0) * 1000:.3f}")

    if len(delivery_latencies) != expected_deliveries:
        failures.append(
            f"Expected {expected_deliveries} deliveries across all subscribers, got {len(delivery_latencies)}"
        )

    if failures:
        print("pubsub failures:")
        for failure in failures[:10]:
            print(f"- {failure}")
        if len(failures) > 10:
            print(f"- ... and {len(failures) - 10} more")
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Redis pub/sub fanout")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--channel", default="perf:pubsub")
    parser.add_argument("--subscribers", type=int, default=20)
    parser.add_argument("--publishers", type=int, default=4)
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--subscription-timeout", type=float, default=10.0)
    parser.add_argument("--delivery-timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
