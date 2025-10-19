// app/api/tables/current/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

// GET - получить текущую таблицу
export async function GET(request) {
  try {
    console.log("🔄 Получение текущей таблицы...");
    
    // Получаем session_id из заголовков
    const sessionId = request.headers.get('x-session-id');
    const cookieHeader = request.headers.get('cookie') || '';
    
    const backendHeaders = {
      'Content-Type': 'application/json',
    };
    
    if (sessionId) {
      backendHeaders['X-Session-ID'] = sessionId;
    }
    
    if (cookieHeader) {
      backendHeaders['Cookie'] = cookieHeader;
    }
    
    const response = await fetch(`${API_URL}/api/tables/current`, {
      method: 'GET',
      headers: backendHeaders,
      credentials: 'include',
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const currentTable = await response.json();
    
    console.log("✅ Текущая таблица:", currentTable);
    
    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json(currentTable);
    const setCookieHeader = response.headers.get('set-cookie');
    if (setCookieHeader) {
      nextResponse.headers.set('set-cookie', setCookieHeader);
    }
    
    return nextResponse;

  } catch (error) {
    console.error("❌ Ошибка при получении текущей таблицы:", error.message);
    return NextResponse.json(
      { error: "Ошибка при получении текущей таблицы" },
      { status: 500 }
    );
  }
}