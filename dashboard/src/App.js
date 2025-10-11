import React, { useEffect, useRef, useState } from "react";
import Globe from "react-globe.gl";
import * as THREE from "three";
import "./App.css";

export default function Globe3D() {
  const globeEl = useRef();
  const infoBoxRef = useRef(null);

  const [paused, setPaused] = useState(false);
  const [layers, setLayers] = useState({
    iss: true,
    wildfire: true,
    aurora: true,
    volcanoe: true,
    earthquake: true,
    severestorm: true,
    flood: true
  });
  const [events, setEvents] = useState([]);
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
    flood: "cyan"
  };

  // -------- Fetch events --------
  const fetchEvents = async () => {
    try {
      const res = await fetch("http://localhost:8000/events");
      const data = await res.json();
      setEvents(data.events);
    } catch (err) {
      console.error("Error fetching events:", err);
    }
  };

  useEffect(() => {
    if (paused) return;
    fetchEvents();
    const interval = setInterval(fetchEvents, 5000);
    return () => clearInterval(interval);
  }, [paused]);

  // -------- Load countries GeoJSON --------
  useEffect(() => {
    fetch("https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson")
      .then(res => res.json())
      .then(data => setCountries(data.features))
      .catch(err => console.error("Error loading countries:", err));
  }, []);

  // -------- Filter events --------
  const filteredEvents = events.filter(e => {
    if (e.type === "iss" && layers.iss) return true;
    if (e.type.includes("fire") && layers.wildfire) return true;
    if (e.type.includes("aurora") && layers.aurora) return true;
    if (e.type.includes("volcanoe") && layers.volcanoe) return true;
    if (e.type.includes("earthquake") && layers.earthquake) return true;
    if (e.type.includes("severestorm") && layers.severestorm) return true;
    if (e.type.includes("flood") && layers.flood) return true;
    return false;
  });

  // -------- Create markers with bigger size and higher altitude --------
  const markers = filteredEvents.map(e => {
    let color = "white";
    let size = 1;
    let shape = "circle";
    let altitude = 0.01;

    switch (e.type) {
      case "iss": color = "magenta"; shape = "circle"; size = 15; altitude = 0.06; break;
      case "fire": color = "red"; shape = "triangle"; size = 12; altitude = 0.04; break;
      case "aurora": color = "lime"; shape = "circle"; size = 10; altitude = 0.03; break;
      case "volcanoe": color = "orange"; shape = "triangle"; size = 14; altitude = 0.05; break;
      case "earthquake": color = "green"; shape = "circle"; size = 16; altitude = 0.06; break;
      case "severestorm": color = "blue"; shape = "triangle"; size = 11; altitude = 0.04; break;
      case "flood": color = "cyan"; shape = "circle"; size = 11; altitude = 0.04; break;
      default: color = "white"; shape = "circle"; size = 10; altitude = 0.03;
    }

    return { ...e, lat: e.latitude, lng: e.longitude, color, size, shape, altitude };
  });

  // -------- Custom 2D markers (visible shapes) --------
  const point2D = d => {
    const mat = new THREE.MeshBasicMaterial({ color: d.color });
    let obj;

    switch (d.shape) {
      case "triangle":
        const triShape = new THREE.Shape();
        triShape.moveTo(0, 1);
        triShape.lineTo(-1, -1);
        triShape.lineTo(1, -1);
        triShape.closePath();
        obj = new THREE.Mesh(new THREE.ShapeGeometry(triShape), mat);
        break;

      case "square":
        const sqShape = new THREE.Shape();
        sqShape.moveTo(-1, 1);
        sqShape.lineTo(1, 1);
        sqShape.lineTo(1, -1);
        sqShape.lineTo(-1, -1);
        sqShape.closePath();
        obj = new THREE.Mesh(new THREE.ShapeGeometry(sqShape), mat);
        break;

      default:
        obj = new THREE.Mesh(new THREE.CircleGeometry(1, 32), mat);
    }

    // face outward
    obj.lookAt(new THREE.Vector3(0, 0, 0));
    // scaled properly
    obj.scale.set(d.size * 0.02, d.size * 0.02, d.size * 0.02);

    return obj;
  };

  // -------- Mouse tracking --------
  useEffect(() => {
    const handleMouse = e => setMousePos({ x: e.clientX, y: e.clientY });
    window.addEventListener("mousemove", handleMouse);
    return () => window.removeEventListener("mousemove", handleMouse);
  }, []);

  // -------- Hide popup outside click --------
  useEffect(() => {
    const handleClickOutside = e => {
      if (infoBoxRef.current && !infoBoxRef.current.contains(e.target)) {
        setHovered(null);
      }
    };
    window.addEventListener("click", handleClickOutside);
    return () => window.removeEventListener("click", handleClickOutside);
  }, []);

  // -------- Fullscreen + camera fit --------
  useEffect(() => {
    if (globeEl.current) {
      globeEl.current.controls().autoRotate = true;
      globeEl.current.controls().autoRotateSpeed = 0.05;
      globeEl.current.camera().position.z = 280;
    }
  }, []);

  return (
    <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, overflow: "hidden", background: "black" }}>
      <Globe
        ref={globeEl}
        globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
        bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
        backgroundImageUrl="//unpkg.com/three-globe/example/img/night-sky.png"
        pointsData={markers}
        pointsMerge={false} // important to see individual shapes
        pointLat="lat"
        pointLng="lng"
        pointColor="color"
        pointAltitude={d => d.altitude}
        pointsTransitionDuration={1000}
        onPointClick={setHovered}
        customThreeObject={point2D}
        polygonsData={countries}
        polygonCapColor={() => "rgba(0,0,0,0)"}
        polygonSideColor={() => "rgba(255,255,255,0.2)"}
        polygonStrokeColor={() => "#ffffff"}
        polygonLabel={d => d.properties.name}
        polygonAltitude={0.002}
      />

      {hovered && (
        <div
          ref={infoBoxRef}
          className="info-box enhanced"
          style={{ left: mousePos.x + 20, top: mousePos.y + 20 }}
        >
          <h3 style={{ margin: "0 0 8px 0", color: hovered.color }}>
            {hovered.type?.toUpperCase()}
          </h3>
          {hovered.satellite_name && <div>Satellite: {hovered.satellite_name}</div>}
          {hovered.velocity_kms && <div>Velocity: {hovered.velocity_kms.toFixed(2)} km/s</div>}
          {hovered.altitude_km && <div>Altitude: {hovered.altitude_km.toFixed(1)} km</div>}
          {hovered.intensity && <div>Intensity: {hovered.intensity}</div>}
          {hovered.description && <div>Description: {hovered.description}</div>}
          <div>Lat: {hovered.latitude?.toFixed(2)}</div>
          <div>Lng: {hovered.longitude?.toFixed(2)}</div>
          {hovered.timestamp && <div>Time: {new Date(hovered.timestamp * 1000).toLocaleString()}</div>}
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
          <h3 style={{ margin: 0 }}>🛰️ Event Controls</h3>
          <button
            onClick={() => setPaused(!paused)}
            style={{ background: paused ? "green" : "red", color: "white", border: "none", borderRadius: 6, padding: "6px 12px", cursor: "pointer" }}
          >
            {paused ? "▶ Resume" : "⏸ Pause"}
          </button>
        </div>

        {Object.keys(layers).map(layer => (
          <label key={layer} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid rgba(255,255,255,0.1)", fontSize: "15px" }}>
            <div style={{ display: "flex", alignItems: "center" }}>
              <div style={{ width: 16, height: 16, borderRadius: "50%", backgroundColor: layerColors[layer], marginRight: 8, boxShadow: `0 0 4px ${layerColors[layer]}` }}></div>
              {layer.charAt(0).toUpperCase() + layer.slice(1)}
            </div>
            <input type="checkbox" checked={layers[layer]} onChange={() => setLayers(prev => ({ ...prev, [layer]: !prev[layer] }))} style={{ transform: "scale(1.3)", cursor: "pointer" }} />
          </label>
        ))}
      </div>
    </div>
  );
}
