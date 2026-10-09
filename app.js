/* Dashboard UI and a small Engine.IO 4 / Socket.IO client implemented on the
 * browser's native WebSocket API. This avoids pulling in socket.io-client while
 * still speaking the protocol used by Flask-SocketIO. */
(() => {
  "use strict";

  const $ = (selector) => document.querySelector(selector);
  const statusDot = $("#socket-status-dot");
  const statusLabel = $("#socket-status-label");
  const pipelineStatus = $("#pipeline-status");
  const updatedTime = $("#updated-time");
  const currencyLabel = $("#currency-label");

  const EVENT_SERIES = [
    { key: "page_views", label: "Page views", color: "#0ea5e9" },
    { key: "product_views", label: "Product views", color: "#6366f1" },
    { key: "add_to_carts", label: "Add to cart", color: "#f59e0b" },
    { key: "purchases", label: "Purchases", color: "#10b981" },
    { key: "searches", label: "Searches", color: "#a855f7" },
  ];

  let eventChart = null;
  let revenueChart = null;
  let latestCurrency = "USD";

  const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

  function numeric(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function formatCurrency(cents, currency) {
    try {
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: currency || "USD",
        maximumFractionDigits: 2,
      }).format(numeric(cents) / 100);
    } catch (_error) {
      return `${currency || "USD"} ${(numeric(cents) / 100).toFixed(2)}`;
    }
  }

  function timeLabel(timestamp) {
    const date = new Date(numeric(timestamp));
    if (Number.isNaN(date.getTime())) return "—";
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function setConnectionState(connected, detail) {
    if (!statusDot || !statusLabel) return;
    statusDot.classList.remove(
      "bg-amber-400",
      "bg-emerald-400",
      "bg-rose-400",
      "shadow-[0_0_10px_rgba(251,191,36,.7)]",
      "shadow-[0_0_10px_rgba(52,211,153,.7)]",
      "shadow-[0_0_10px_rgba(251,113,133,.7)]",
    );
    statusDot.classList.add(
      connected ? "bg-emerald-400" : detail === "error" ? "bg-rose-400" : "bg-amber-400",
      connected
        ? "shadow-[0_0_10px_rgba(52,211,153,.7)]"
        : detail === "error"
          ? "shadow-[0_0_10px_rgba(251,113,133,.7)]"
          : "shadow-[0_0_10px_rgba(251,191,36,.7)]",
    );
    statusLabel.textContent = connected ? "Live stream connected" : detail === "error" ? "Stream retrying" : "Reconnecting";

    if (pipelineStatus) {
      pipelineStatus.textContent = connected
        ? "The WebSocket stream is connected. Metrics arrive continuously from the Redis aggregation window."
        : detail === "error"
          ? "The realtime stream is reconnecting. The dashboard will continue trying automatically."
          : "Waiting for the realtime event stream; the dashboard will reconnect automatically.";
    }
  }

  function createCharts() {
    if (typeof window.Chart !== "function") {
      if (pipelineStatus) {
        pipelineStatus.textContent = "Metrics API is available, but the Chart.js CDN did not load; charts cannot be drawn.";
      }
      return;
    }

    Chart.defaults.font.family = "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
    Chart.defaults.color = "#64748b";

    const commonOptions = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      animation: { duration: 250 },
      plugins: {
        legend: {
          position: "bottom",
          labels: { usePointStyle: true, pointStyle: "circle", boxWidth: 7, boxHeight: 7, padding: 18, font: { size: 11 } },
        },
        tooltip: {
          backgroundColor: "#0f172a",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          padding: 12,
          cornerRadius: 10,
          displayColors: true,
        },
      },
      scales: {
        x: {
          grid: { display: false, drawBorder: false },
          border: { display: false },
          ticks: { maxTicksLimit: 8, maxRotation: 0, autoSkip: true, font: { size: 10 } },
        },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(148, 163, 184, 0.16)", drawBorder: false },
          border: { display: false, dash: [3, 4] },
          ticks: { precision: 0, maxTicksLimit: 5, font: { size: 10 } },
        },
      },
    };

    const eventCanvas = $("#events-chart");
    const revenueCanvas = $("#revenue-chart");
    if (!eventCanvas || !revenueCanvas) return;

    eventChart = new Chart(eventCanvas, {
      type: "line",
      data: {
        labels: [],
        datasets: EVENT_SERIES.map((series) => ({
          label: series.label,
          data: [],
          borderColor: series.color,
          backgroundColor: series.color,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.32,
          fill: false,
        })),
      },
      options: commonOptions,
    });

    revenueChart = new Chart(revenueCanvas, {
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label: "Gross revenue",
            data: [],
            borderColor: "#0f766e",
            backgroundColor: "rgba(20, 184, 166, 0.12)",
            borderWidth: 2.5,
            pointRadius: 0,
            pointHoverRadius: 4,
            tension: 0.32,
            fill: true,
          },
        ],
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (context) => `${context.dataset.label}: ${formatCurrency(context.parsed.y * 100, latestCurrency)}`,
            },
          },
        },
        scales: {
          ...commonOptions.scales,
          y: {
            ...commonOptions.scales.y,
            ticks: {
              ...commonOptions.scales.y.ticks,
              callback: (value) => formatCurrency(Number(value) * 100, latestCurrency),
            },
          },
        },
      },
    });
  }

  function applyMetrics(snapshot) {
    if (!snapshot || snapshot.status !== "ok") return;
    const totals = snapshot.totals || {};
    const series = Array.isArray(snapshot.series) ? snapshot.series : [];
    const currency = typeof snapshot.currency === "string" ? snapshot.currency : "USD";
    latestCurrency = currency;

    const activeUsers = $("#active-users");
    const totalEvents = $("#total-events");
    const totalPurchases = $("#total-purchases");
    const grossRevenue = $("#gross-revenue");
    if (activeUsers) activeUsers.textContent = numberFormatter.format(numeric(totals.active_users));
    if (totalEvents) totalEvents.textContent = numberFormatter.format(numeric(totals.events_total));
    if (totalPurchases) totalPurchases.textContent = numberFormatter.format(numeric(totals.purchases));
    if (grossRevenue) grossRevenue.textContent = formatCurrency(totals.revenue_cents, currency);
    if (currencyLabel) currencyLabel.textContent = currency;

    const windowLabel = $("#window-label");
    if (windowLabel) windowLabel.textContent = `${numeric(snapshot.bucket_seconds) || 10} seconds`;
    const serverTime = numeric(snapshot.server_time_ms);
    if (updatedTime) updatedTime.textContent = serverTime ? timeLabel(serverTime) : "Just now";

    if (!eventChart || !revenueChart) return;
    const labels = series.map((point) => timeLabel(point.timestamp_ms));
    eventChart.data.labels = labels;
    EVENT_SERIES.forEach((eventSeries, index) => {
      eventChart.data.datasets[index].data = series.map((point) => numeric(point[eventSeries.key]));
    });
    eventChart.update("none");

    revenueChart.data.labels = labels;
    revenueChart.data.datasets[0].data = series.map((point) => numeric(point.revenue_cents) / 100);
    revenueChart.update("none");
  }

  async function fetchSnapshot() {
    try {
      const response = await fetch("/api/metrics", {
        method: "GET",
        cache: "no-store",
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error(`Metrics API returned ${response.status}`);
      applyMetrics(await response.json());
    } catch (error) {
      if (pipelineStatus) {
        pipelineStatus.textContent = "The metrics API is temporarily unavailable; automatic retries are active.";
      }
      console.warn("Unable to fetch analytics snapshot:", error);
    }
  }

  class NativeSocketIOClient {
    constructor() {
      this.socket = null;
      this.connected = false;
      this.retryDelay = 1000;
      this.reconnectTimer = null;
      this.livenessTimer = null;
      this.closedByClient = false;
    }

    connect() {
      if (this.socket && (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING)) return;
      this.closedByClient = false;
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const endpoint = new URL(`${protocol}//${window.location.host}/socket.io/`);
      endpoint.searchParams.set("EIO", "4");
      endpoint.searchParams.set("transport", "websocket");

      try {
        this.socket = new WebSocket(endpoint.toString());
        this.socket.onopen = () => {
          this.armLivenessWatchdog(60000);
        };
        this.socket.onmessage = (event) => this.handleFrame(event.data);
        this.socket.onerror = () => {
          setConnectionState(false, "error");
        };
        this.socket.onclose = () => {
          this.connected = false;
          this.clearTimers();
          setConnectionState(false, "retry");
          if (!this.closedByClient) this.scheduleReconnect();
        };
      } catch (error) {
        console.warn("Unable to create WebSocket:", error);
        setConnectionState(false, "error");
        this.scheduleReconnect();
      }
    }

    handleFrame(data) {
      if (typeof data !== "string") return;
      // Engine.IO v4 may batch packets with the record-separator character.
      for (const packet of data.split("\x1e")) {
        if (!packet) continue;
        this.armLivenessWatchdog(60000);
        const type = packet.charAt(0);

        if (type === "0") {
          let handshake = {};
          try {
            handshake = JSON.parse(packet.slice(1));
          } catch (_error) {
            console.warn("Invalid Engine.IO open packet");
          }
          const timeout = numeric(handshake.pingInterval) + numeric(handshake.pingTimeout) + 10000;
          this.armLivenessWatchdog(Math.max(timeout, 30000));
          this.send("40"); // Engine.IO MESSAGE + Socket.IO CONNECT (default namespace).
          continue;
        }

        if (type === "1") {
          this.socket?.close();
          continue;
        }

        if (type === "2") {
          // Engine.IO PING; echo the optional payload in the matching PONG.
          this.send(`3${packet.slice(1)}`);
          continue;
        }

        if (type !== "4") continue;
        const socketPacket = packet.slice(1);
        const socketType = socketPacket.charAt(0);

        if (socketType === "0") {
          this.connected = true;
          this.retryDelay = 1000;
          setConnectionState(true);
        } else if (socketType === "1") {
          this.connected = false;
          this.socket?.close();
        } else if (socketType === "2") {
          try {
            const [eventName, payload] = JSON.parse(socketPacket.slice(1));
            if (eventName === "metrics") applyMetrics(payload);
          } catch (error) {
            console.warn("Could not decode Socket.IO event:", error);
          }
        } else if (socketType === "4") {
          console.warn("Socket.IO namespace connection was rejected:", socketPacket.slice(1));
          setConnectionState(false, "error");
          this.socket?.close();
        }
      }
    }

    send(packet) {
      if (this.socket && this.socket.readyState === WebSocket.OPEN) {
        this.socket.send(packet);
      }
    }

    armLivenessWatchdog(timeout) {
      window.clearTimeout(this.livenessTimer);
      this.livenessTimer = window.setTimeout(() => {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) this.socket.close();
      }, timeout);
    }

    scheduleReconnect() {
      if (this.reconnectTimer || this.closedByClient) return;
      const delay = this.retryDelay + Math.floor(Math.random() * 300);
      this.reconnectTimer = window.setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, delay);
      this.retryDelay = Math.min(Math.round(this.retryDelay * 1.8), 30000);
    }

    clearTimers() {
      window.clearTimeout(this.reconnectTimer);
      window.clearTimeout(this.livenessTimer);
      this.reconnectTimer = null;
      this.livenessTimer = null;
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    createCharts();
    fetchSnapshot();

    const socketClient = new NativeSocketIOClient();
    socketClient.connect();

    // REST polling is a low-frequency fallback while the native WebSocket reconnects.
    window.setInterval(() => {
      if (!socketClient.connected) fetchSnapshot();
    }, 5000);
  });
})();
