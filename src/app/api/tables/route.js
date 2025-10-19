// app/api/tables/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

// GET - получить список таблиц
export async function GET(request) {
  try {
    console.log("🔄 Получение списка таблиц...");
    
    // Получаем session_id из заголовков
    const sessionId = request.headers.get('x-session-id');
    const cookieHeader = request.headers.get('cookie') || '';
    
    console.log("Session ID from headers:", sessionId);
    
    const backendHeaders = {
      'Content-Type': 'application/json',
    };
    
    if (sessionId) {
      backendHeaders['X-Session-ID'] = sessionId;
    }
    
    if (cookieHeader) {
      backendHeaders['Cookie'] = cookieHeader;
    }
    
    const response = await fetch(`${API_URL}/api/tables`, {
      method: 'GET',
      headers: backendHeaders,
      credentials: 'include',
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const tables = await response.json();
    
    console.log("✅ Таблицы получены:", tables);
    
    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json({
      tables: tables,
      total: tables.length,
    });
    
    const setCookieHeader = response.headers.get('set-cookie');
    if (setCookieHeader) {
      nextResponse.headers.set('set-cookie', setCookieHeader);
    }
    
    return nextResponse;
  } catch (error) {
    console.error("❌ Ошибка при получении таблиц:", error.message);
    return NextResponse.json(
      { error: "Ошибка при получении таблиц" },
      { status: 500 }
    );
  }
}