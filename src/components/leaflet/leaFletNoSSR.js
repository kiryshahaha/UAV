"use client";

import dynamic from "next/dynamic";

// компоненты без SSR
export const MapContainer = dynamic(
  () => import("react-leaflet").then((mod) => mod.MapContainer),
  { ssr: false }
);
export const TileLayer = dynamic(
  () => import("react-leaflet").then((mod) => mod.TileLayer),
  { ssr: false }
);
export const GeoJSON = dynamic(
  () => import("react-leaflet").then((mod) => mod.GeoJSON),
  { ssr: false }
);
export const Marker = dynamic(
  () => import("react-leaflet").then((mod) => mod.Marker),
  { ssr: false }
);
export const Popup = dynamic(
  () => import("react-leaflet").then((mod) => mod.Popup),
  { ssr: false }
);

// ЭКСПОРТ КРУГА БЫЛ ДОБАВЛЕН ЗДЕСЬ
export const Circle = dynamic(
  () => import("react-leaflet").then((mod) => mod.Circle),
  { ssr: false }
);

export const MarkerClusterGroup = dynamic(
  () => import("react-leaflet-markercluster").then((mod) => mod.default),
  { ssr: false }
);

export const L = typeof window !== "undefined" ? require("leaflet") : null;

export const Tooltip = dynamic(
  () => import("react-leaflet").then((mod) => mod.Tooltip),
  { ssr: false }
);