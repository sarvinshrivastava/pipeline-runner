/**
 * Bounded log capture for a single run.
 *
 * Keeps the HEAD and the TAIL rather than simply stopping at the cap. The head
 * shows how a run started (dependency install, config echo); the tail holds the
 * error that actually killed it. Truncating either end alone reliably discards
 * the half you need.
 */
import { appendFileSync, writeFileSync } from "node:fs";

export class LogSink {
  private headBytes = 0;
  private tail: Buffer[] = [];
  private tailBytes = 0;
  private dropped = 0;

  private readonly headCap: number;
  private readonly tailCap: number;

  constructor(private readonly path: string, maxBytes: number) {
    this.headCap = Math.floor(maxBytes / 2);
    this.tailCap = maxBytes - this.headCap;
    writeFileSync(this.path, "");
  }

  write(chunk: Uint8Array): void {
    let buf = Buffer.from(chunk);

    if (this.headBytes < this.headCap) {
      const room = this.headCap - this.headBytes;
      const head = buf.subarray(0, room);
      appendFileSync(this.path, head);
      this.headBytes += head.length;
      buf = buf.subarray(head.length);
      if (buf.length === 0) return;
    }

    this.tail.push(buf);
    this.tailBytes += buf.length;
    while (this.tailBytes > this.tailCap && this.tail.length > 1) {
      const shifted = this.tail.shift()!;
      this.tailBytes -= shifted.length;
      this.dropped += shifted.length;
    }
  }

  writeText(text: string): void {
    this.write(Buffer.from(text, "utf8"));
  }

  /** Flushes the retained tail. Safe to call once. */
  close(): void {
    if (this.dropped > 0) {
      appendFileSync(
        this.path,
        `\n\n... ${this.dropped} bytes of output dropped from the middle ...\n\n`,
      );
    }
    for (const chunk of this.tail) appendFileSync(this.path, chunk);
    this.tail = [];
    this.tailBytes = 0;
  }
}
