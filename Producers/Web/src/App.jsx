import { useEffect, useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

function App() {
  const [seismicData, setSeismicData] = useState(null);

  useEffect(() => {
    fetch('http://localhost:5000/seismic_events')
      .then(response => {
        if (!response.ok) {
          throw new Error('HTTP error!');
        }
        return response.json();
      })
      .then(data => setSeismicData(data))
      .catch(() => {});
  }, []);

  return (
    <MapContainer center={[0, 0]} zoom={2} style={{ height: "100vh", width: "100%" }}>
      <TileLayer
        attribution='Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
        url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
      />
      {seismicData && seismicData.map((quake) => {
        const coordinates = JSON.parse(quake.co_ordinates);
        return (
          <CircleMarker
            key={quake.time}
            center={[coordinates[1], coordinates[0]]}
            radius={Math.max(quake.magnitude * 3, 5.5)}
            fillOpacity={0.5}
            pathOptions={quake.magnitude >= 3.5 ? { color: 'red' } : {}}
            eventHandlers={{
              mouseover: (e) => {
                e.target.openPopup();
              },
              mouseout: (e) => {
                e.target.closePopup();
              },
            }}
          >
            <Popup>
              <div>
                <h4>Magnitude: {quake.magnitude}</h4>
                <p>{quake.region}</p>
                <p>Time: {quake.time.replace("T", " at ").replace("Z", " UTC")}</p>
              </div>
            </Popup>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}

export default App;