import { NextResponse } from "next/server";

async function searchCities(searchTerm) {
  // const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://backend:8000'
  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000"; // Для локального запуска

  try {
    const url = searchTerm
      ? `${API_URL}/cities?search=${encodeURIComponent(searchTerm)}`
      : `${API_URL}/cities`;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000); // Увеличиваем до 10 секунд

    const response = await fetch(url, {
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (response.ok) {
      const data = await response.json();
      console.log("✅ Города получены с бэкенда:", data.cities?.length || 0);
      return data.cities || [];
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (error) {
    console.error("❌ Ошибка при получении городов с бэкенда:", error.message);
    return [];
  }
}

export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const searchTerm = searchParams.get("search")?.toLowerCase() || "";

  try {
    const cities = await searchCities(searchTerm);

    console.log(`Найдено городов для "${searchTerm}":`, cities.length);

    return NextResponse.json({
      cities: cities,
      total: cities.length,
    });
  } catch (error) {
    console.error("Error in cities API:", error);
    return NextResponse.json({
      cities: [],
      total: 0,
    });
  }
}
