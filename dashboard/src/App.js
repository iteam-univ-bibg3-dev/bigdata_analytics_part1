import React, { useEffect, useState } from "react";
import Globe3D from "./Globe3D";
import "./App.css";

export default function App() {
  // ---------------- State ----------------
  const [pausedGlobe, setPausedGlobe] = useState(false); // pause globe refresh
  const [pausedLogs, setPausedLogs] = useState(false);   // pause logs streaming

  const [layers, setLayers] = useState({
    iss: true,
    wildfire: true,
    aurora: true,
    volcanoe: true,
    earthquake: true,
    severestorm: true,
    flood: true,
  });

  const [events, setEvents] = useState([]);
  const [logs, setLogs] = useState([]);
  const [hovered, setHovered] = useState(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const [countries, setCountries] = useState([]);

  const layerColors = {
    iss: "magenta",
    wildfire: "red",
    aurora: "lime",
    volcanoe: "orange",
    earthquake: "green",
    severestorm: "blue",
    flood: "cyan",
  };

  // ---------------- Mouse Tracking ----------------
  useEffect(() => {
    const handleMouse = (e) => setMousePos({ x: e.clientX, y: e.clientY });
    window.addEventListener("mousemove", handleMouse);
    return () => window.removeEventListener("mousemove", handleMouse);
  }, []);

  // ---------------- Hide Hover Popup ----------------
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (hovered && !e.target.closest(".info-box")) setHovered(null);
    };
    window.addEventListener("click", handleClickOutside);
    return () => window.removeEventListener("click", handleClickOutside);
  }, [hovered]);

  // ---------------- Fetch Events from Elasticsearch ----------------
  useEffect(() => {
    if (pausedGlobe) return;

    const fetchEvents = async () => {
      try {
        const res = await fetch("http://localhost:9200/nature_data/_search?size=200");
        const data = await res.json();
        const hits = data.hits?.hits || [];
        const esEvents = hits.map(h => {
          const src = h._source || {};
          return {
            type: src.type,
            latitude: Number(src.latitude ?? src.geometry?.lat),
            longitude: Number(src.longitude ?? src.geometry?.lon),
            timestamp: src.timestamp,
            description: src.description || "",
          };
        });
        setEvents(esEvents.filter(e => !isNaN(e.latitude) && !isNaN(e.longitude)));

      } catch (err) {
        console.error("Error fetching events for Globe:", err);
      }
    };

    fetchEvents();
    const interval = setInterval(fetchEvents, 5000); // refresh every 5s
    return () => clearInterval(interval);
  }, [pausedGlobe]);

  // ---------------- Kafka WebSocket Logs ----------------
  useEffect(() => {
    const wsUrl = process.env.REACT_APP_WS_URL || "ws://localhost:8081";
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => console.log("Connected to Kafka WebSocket");

    ws.onmessage = (message) => {
      if (pausedLogs) return; // skip if logs paused
      try {
        const log = JSON.parse(message.data);
        setLogs(prev => [...prev, log].slice(-200));
      } catch (err) {
        console.error("Error parsing Kafka log:", err);
      }
    };

    ws.onclose = () => console.log("WebSocket disconnected");

    return () => ws.close();
  }, [pausedLogs]);
  // Load previous logs on startup
  useEffect(() => {
    const saved = localStorage.getItem("kafka_logs");
    if (saved) setLogs(JSON.parse(saved));
  }, []);


  // ---------------- Filter Events by Layers ----------------
  const filteredEvents = events.filter((e) => {
    if (e.type === "iss" && layers.iss) return true;
    if (e.type.includes("fire") && layers.wildfire) return true;
    if (e.type.includes("aurora") && layers.aurora) return true;
    if (e.type.includes("volcanoe") && layers.volcanoe) return true;
    if (e.type.includes("earthquake") && layers.earthquake) return true;
    if (e.type.includes("severestorm") && layers.severestorm) return true;
    if (e.type.includes("flood") && layers.flood) return true;
    return false;
  });

  return (
    <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, overflow: "hidden", background: "black" }}>
      {/* -------- 3D Globe -------- */}
      <Globe3D
        events={filteredEvents}
        countries={countries}
        hovered={hovered}
        setHovered={setHovered}
      />

      {/* -------- Hover Info Box -------- */}
      {hovered && (
        <div
          className="info-box enhanced"
          style={{ left: mousePos.x + 20, top: mousePos.y + 20 }}
        >
          <h3 style={{ margin: "0 0 8px 0", color: layerColors[hovered.type] || "white" }}>
            {hovered.type?.toUpperCase()}
          </h3>
          {hovered.description && <div>{hovered.description}</div>}
          <div>Lat: {hovered.latitude?.toFixed(2)}</div>
          <div>Lng: {hovered.longitude?.toFixed(2)}</div>
          {hovered.timestamp && (
            <div>Time: {new Date(hovered.timestamp * 1000).toLocaleString()}</div>
          )}
        </div>
      )}

      {/* -------- Control Panel -------- */}
      <div className="controls enhanced" style={{
        position: "absolute",
        top: 20,
        right: 20,
        background: "rgba(0,0,0,0.65)",
        borderRadius: 12,
        padding: "12px 16px",
        color: "white",
        maxHeight: "90vh",
        overflowY: "auto",
        fontFamily: "sans-serif",
        width: 260
      }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
          <h3 style={{ margin: 0 }}>🛰️ Controls</h3>
        </div>

        {/* Globe Pause */}
        <button
          onClick={() => setPausedGlobe(!pausedGlobe)}
          style={{
            background: pausedGlobe ? "green" : "red",
            color: "white",
            border: "none",
            borderRadius: 6,
            padding: "6px 12px",
            marginBottom: 12,
            cursor: "pointer"
          }}
        >
          {pausedGlobe ? "▶ Resume Globe" : "⏸ Pause Globe"}
        </button>

        {/* Layer Toggles */}
        {Object.keys(layers).map((layer) => (
          <label key={layer} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid rgba(255,255,255,0.1)", fontSize: "15px" }}>
            <div style={{ display: "flex", alignItems: "center" }}>
              <div style={{ width: 16, height: 16, borderRadius: "50%", backgroundColor: layerColors[layer], marginRight: 8, boxShadow: `0 0 4px ${layerColors[layer]}` }}></div>
              {layer.charAt(0).toUpperCase() + layer.slice(1)}
            </div>
            <input type="checkbox" checked={layers[layer]} onChange={() => setLayers(prev => ({ ...prev, [layer]: !prev[layer] }))} style={{ transform: "scale(1.3)", cursor: "pointer" }} />
          </label>
        ))}
      </div>

      {/* -------- Kafka Logs Panel -------- */}
      <div className="logs-panel" style={{
        position: "absolute",
        bottom: 20,
        right: 20,
        width: 320,
        maxHeight: "45vh",
        overflowY: "auto",
        background: "rgba(0,0,0,0.65)",
        borderRadius: 12,
        padding: 12,
        color: "white",
        fontFamily: "monospace",
        fontSize: 14
      }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
          <h4 style={{ margin: 0, fontSize: 16 }}>📡 Kafka Logs</h4>
          <button
            onClick={() => setPausedLogs(!pausedLogs)}
            style={{
              background: pausedLogs ? "green" : "red",
              color: "white",
              border: "none",
              borderRadius: 6,
              padding: "4px 10px",
              fontSize: 13,
              cursor: "pointer"
            }}
          >
            {pausedLogs ? "▶ Resume" : "⏸ Pause"}
          </button>
        </div>
          <div style={{ maxHeight: "calc(45vh - 32px)", overflowY: "auto" }}>
            {logs.slice(-50).map((log, i) => {
              let parsed = log;
              if (typeof log === "string") {
                try {
                  parsed = JSON.parse(log);
                } catch {
                  parsed = { message: log };
                }
              }

              const type = parsed.type?.toLowerCase?.() || "unknown";
              const message =
                parsed.message ||
                parsed.event ||
                JSON.stringify(parsed, null, 0);

              const typeColors = {
                fire: "#ff4500",
                wildfire: "#ff6347",
                flood: "#1e90ff",
                storm: "#9370db",
                earthquake: "#ffa500",
                volcano: "#ff1493",
                aurora: "#00ff7f",
                iss: "#ff00ff",
                unknown: "#ccc"
              };

              return (
                <div
                  key={i}
                  style={{
                    marginBottom: 4,
                    fontSize: 13,
                    background: "rgba(255,255,255,0.08)",
                    borderRadius: 4,
                    padding: "4px 6px",
                    color: typeColors[type] || "#ccc",
                  }}
                >
                  <strong>[{type.toUpperCase()}]</strong> {message}
                </div>
              );
            })}


          </div>

      </div>


    </div>
  );
}
