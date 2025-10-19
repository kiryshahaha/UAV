// app/api/operators/[type]/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

export async function GET(request, { params }) {
  const { type } = params;
  const { searchParams } = new URL(request.url);
  const date_from = searchParams.get("date_from");
  const date_to = searchParams.get("date_to");
  const region = searchParams.get("region"); 
  const limit = searchParams.get("limit") || 50;

  try {
    // Получаем session_id из заголовков фронтенда
    const sessionId = request.headers.get('x-session-id');
    const cookieHeader = request.headers.get('cookie') || '';
    
    console.log("📊 Fetching operators data for type:", type, "Session ID:", sessionId);
    
    const backendHeaders = {
      'Content-Type': 'application/json',
    };
    
    // Передаем session_id в заголовках
    if (sessionId) {
      backendHeaders['X-Session-ID'] = sessionId;
    }
    
    // Также передаем cookies для обратной совместимости
    if (cookieHeader) {
      backendHeaders['Cookie'] = cookieHeader;
    }
    
    // Строим URL для запроса к FastAPI
    const url = new URL(`${API_URL}/operators/top/${type}`);
    
    if (date_from) url.searchParams.append("date_from", date_from);
    if (date_to) url.searchParams.append("date_to", date_to);
    if (region) url.searchParams.append("region", region);
    if (limit) url.searchParams.append("limit", limit);

    console.log("📡 Forwarding to backend:", url.toString());

    const response = await fetch(url, {
      method: "GET",
      headers: backendHeaders,
      credentials: 'include',
    });
    
    if (response.ok) {
      const data = await response.json();
      console.log(`✅ Operators data received for type: ${type}, records: ${data.operators?.length || 0}`);
      
      // Копируем cookies из ответа бэкенда
      const nextResponse = NextResponse.json(data);
      const setCookieHeader = response.headers.get('set-cookie');
      if (setCookieHeader) {
        nextResponse.headers.set('set-cookie', setCookieHeader);
      }
      
      return nextResponse;
    } else {
      const errorText = await response.text();
      console.error("❌ Backend API error:", response.status, errorText);
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }
  } catch (error) {
    console.error("❌ Operators API route error:", error.message);
    return NextResponse.json(
      {
        error: "Failed to fetch operators data",
        details: error.message,
      },
      { status: 500 }
    );
  }
}