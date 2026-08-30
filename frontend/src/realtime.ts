export type ConnectionStatus = 'LIVE' | 'RECONNECTING' | 'OFFLINE';

export interface EventEnvelope {
  seq: number;
  type: string;
  schema_version: number;
  server_version: string;
  frontend_build_id: string;
  timestamp: string;
  realtime_alert_eligible?: boolean;
  classification_source?: string | null;
  chain_finality?: 'PENDING' | 'CONFIRMED' | 'ORPHANED' | null;
  trade_evidence?: 'STRONG' | 'CONFIRMED' | 'REJECTED' | 'INTEGRITY_CONFLICT' | null;
  signal_finality?: 'PENDING' | 'CONFIRMED' | null;
  provisional?: boolean;
  data: unknown;
}

interface Hello {
  type: 'system.hello';
  current_outbox_seq: number;
  server_version: string;
  frontend_build_id: string;
  api_schema_version: number;
}

interface ReplayPage {
  events: EventEnvelope[];
  next_seq: number;
  high_watermark: number;
  has_more: boolean;
}

export class SequenceStore {
  private seen: number;
  constructor(initial: number, private readonly persist: (seq: number) => void) { this.seen = initial; }
  get lastSeen() { return this.seen; }
  accept(event: EventEnvelope) {
    if (event.seq <= this.seen) return false;
    this.seen = event.seq;
    this.persist(this.seen);
    return true;
  }
}

export function reconnectDelay(attempt: number, random = Math.random()) {
  const delays = [1000, 2000, 4000, 8000, 15000, 30000];
  const base = delays[Math.min(Math.max(attempt, 0), delays.length - 1)];
  return base + Math.floor(base * (random * 0.4 - 0.2));
}

export class RealtimeClient {
  private socket?: WebSocket;
  private stopped = false;
  private attempt = 0;
  private buffered: EventEnvelope[] = [];
  private replaying = false;
  private readonly sequence: SequenceStore;

  constructor(
    private readonly onStatus: (status: ConnectionStatus) => void,
    private readonly onEvent: (event: EventEnvelope) => void,
    private readonly onServer: (version: string, build: string, apiSchema: number) => void,
    storage: Storage = sessionStorage,
  ) {
    const initial = Number.parseInt(storage.getItem('pons.last_seen_seq') ?? '0', 10);
    this.sequence = new SequenceStore(Number.isSafeInteger(initial) ? initial : 0, (seq) => storage.setItem('pons.last_seen_seq', String(seq)));
  }

  get lastSeenSeq() { return this.sequence.lastSeen; }

  start() { this.stopped = false; this.connect(); }
  stop() { this.stopped = true; this.socket?.close(); this.onStatus('OFFLINE'); }

  private connect() {
    if (this.stopped) return;
    this.onStatus(this.attempt === 0 ? 'RECONNECTING' : 'OFFLINE');
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socket = new WebSocket(`${protocol}//${location.host}/ws`);
    this.socket = socket;
    socket.onmessage = (message) => {
      void this.receive(JSON.parse(String(message.data)) as Hello | EventEnvelope).catch(() => socket.close());
    };
    socket.onclose = () => this.scheduleReconnect();
    socket.onerror = () => socket.close();
  }

  private async receive(message: Hello | EventEnvelope) {
    if ('current_outbox_seq' in message) {
      this.onServer(message.server_version, message.frontend_build_id, message.api_schema_version);
      this.replaying = true;
      await this.replayThrough(message.current_outbox_seq);
      this.replaying = false;
      this.buffered.sort((a, b) => a.seq - b.seq).forEach((event) => this.apply(event));
      this.buffered = [];
      this.attempt = 0;
      this.onStatus('LIVE');
      return;
    }
    if (this.replaying) this.buffered.push(message);
    else if (message.seq > this.sequence.lastSeen + 1) {
      this.replaying = true;
      this.buffered.push(message);
      await this.replayThrough(message.seq - 1);
      this.replaying = false;
      this.buffered.sort((a, b) => a.seq - b.seq).forEach((event) => this.apply(event));
      this.buffered = [];
    } else this.apply(message);
  }

  private async replayThrough(high: number) {
    while (this.sequence.lastSeen < high) {
      const before = this.sequence.lastSeen;
      const response = await fetch(`/api/v1/events?after_seq=${this.sequence.lastSeen}&through_seq=${high}&limit=200`, { credentials: 'same-origin' });
      if (!response.ok) throw new Error(`event replay returned ${response.status}`);
      const page = await response.json() as ReplayPage;
      page.events.forEach((event) => this.apply(event));
      if (!page.has_more || page.next_seq <= before) break;
    }
  }

  private apply(event: EventEnvelope) { if (this.sequence.accept(event)) this.onEvent(event); }

  private scheduleReconnect() {
    if (this.stopped) return;
    this.onStatus(navigator.onLine ? 'RECONNECTING' : 'OFFLINE');
    window.setTimeout(() => this.connect(), reconnectDelay(this.attempt++));
  }
}
