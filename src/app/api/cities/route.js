import { NextResponse } from "next/server";

export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const searchTerm = searchParams.get("search")?.toLowerCase() || "";

  try {
    const API_URL = process.env.NEXT_PUBLIC_API_URL;
    const url = searchTerm
      ? `${API_URL}/cities?search=${encodeURIComponent(searchTerm)}`
      : `${API_URL}/cities`;

    const response = await fetch(url);
    
    if (response.ok) {
      const data = await response.json();
      
      // ПРЕОБРАЗУЕМ ДАННЫЕ: regions -> cities
      const cities = data.regions || [];
      console.log("✅ Города получены с бэкенда:", cities.length);
      
      return NextResponse.json({
        cities: cities,  // Теперь фронтенд получит cities
        total: cities.length,
      });
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (error) {
    console.error("❌ Ошибка при получении городов с бэкенда:", error.message);
    return NextResponse.json({
      cities: [],
      total: 0,
    });
  }
}
