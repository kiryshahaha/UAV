// app/api/dashboard/stats/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);
    const date_from = searchParams.get('date_from');
    const date_to = searchParams.get('date_to');
    const region = searchParams.get('region');
    
    // Получаем session_id из заголовков
    const sessionId = request.headers.get('x-session-id');
    const cookieHeader = request.headers.get('cookie') || '';
    
    console.log("🔄 /dashboard/stats - Session ID:", sessionId);
    
    const backendHeaders = {
      'Content-Type': 'application/json',
    };
    
    if (sessionId) {
      backendHeaders['X-Session-ID'] = sessionId;
    }
    
    if (cookieHeader) {
      backendHeaders['Cookie'] = cookieHeader;
    }
    
    const params = new URLSearchParams();
    if (date_from) params.append('date_from', date_from);
    if (date_to) params.append('date_to', date_to);
    if (region) params.append('region', region);
    
    const queryString = params.toString();
    const url = `${API_URL}/dashboard/stats${queryString ? `?${queryString}` : ''}`;
    
    console.log("📡 Forwarding to backend:", url);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: backendHeaders,
      credentials: 'include',
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const data = await response.json();
    
    console.log("✅ /dashboard/stats - данные получены");
    
    return NextResponse.json(data);
    
  } catch (error) {
    console.error("❌ Ошибка в /api/dashboard/stats:", error.message);
    return NextResponse.json(
      { error: "Ошибка при получении статистики дашборда: " + error.message },
      { status: 500 }
    );
  }
}