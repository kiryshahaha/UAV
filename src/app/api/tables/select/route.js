// app/api/tables/select/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

export async function POST(request) {
  try {
    const { table_name } = await request.json();
    
    // Получаем session_id из заголовков фронтенда
    const sessionId = request.headers.get('x-session-id');
    
    console.log("🔄 Выбор таблицы:", table_name, "session_id из заголовков:", sessionId);
    
    const backendHeaders = {
      'Content-Type': 'application/json',
    };
    
    // Передаем session_id из заголовков фронтенда
    if (sessionId) {
      backendHeaders['X-Session-ID'] = sessionId;
    }
    
    // Также передаем cookies для обратной совместимости
    const cookieHeader = request.headers.get('cookie');
    if (cookieHeader) {
      backendHeaders['Cookie'] = cookieHeader;
    }
    
    const response = await fetch(`${API_URL}/api/tables/select`, {
      method: 'POST',
      headers: backendHeaders,
      body: JSON.stringify({ table_name }),
      credentials: 'include',
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const result = await response.json();
    
    console.log("✅ Таблица выбрана:", result);
    
    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json(result);
    const setCookieHeader = response.headers.get('set-cookie');
    if (setCookieHeader) {
      nextResponse.headers.set('set-cookie', setCookieHeader);
    }
    
    return nextResponse;
  } catch (error) {
    console.error("❌ Ошибка при выборе таблицы:", error.message);
    return NextResponse.json(
      { error: "Ошибка при выборе таблицы: " + error.message },
      { status: 500 }
    );
  }
}