// app/api/stats/regions/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

async function fetchRegionsData(dateFrom, dateTo, sessionId, cookieHeader) {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    // Строим URL с параметрами
    const url = new URL(`${API_URL}/stats/regions`);
    if (dateFrom) url.searchParams.append('date_from', dateFrom);
    if (dateTo) url.searchParams.append('date_to', dateTo);

    console.log("📡 Запрос статистики регионов к бэкенду:", url.toString());

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

    const response = await fetch(url.toString(), {
      signal: controller.signal,
      headers: backendHeaders,
      credentials: 'include',
    });

    clearTimeout(timeoutId);

    if (response.ok) {
      const data = await response.json();
      console.log("✅ Данные регионов получены с бэкенда:", data.length || 0);
      return data || [];
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (error) {
    console.error("❌ Ошибка при получении данных регионов с бэкенда:", error.message);
    return [];
  }
}

export async function GET(request) {
  try {
    // Получаем параметры из запроса
    const { searchParams } = new URL(request.url);
    const dateFrom = searchParams.get('date_from');
    const dateTo = searchParams.get('date_to');

    // Получаем session_id из заголовков фронтенда
    const sessionId = request.headers.get('x-session-id');
    const cookieHeader = request.headers.get('cookie') || '';

    console.log(`📊 Запрос статистики регионов: date_from=${dateFrom}, date_to=${dateTo}, session_id=${sessionId}`);

    const regionsData = await fetchRegionsData(dateFrom, dateTo, sessionId, cookieHeader);

    console.log(`✅ Получено данных о регионах:`, regionsData.length);

    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json(regionsData);
    // Note: Бэкенд может вернуть set-cookie, который нужно проксировать
    // const setCookieHeader = response.headers.get('set-cookie');
    // if (setCookieHeader) {
    //   nextResponse.headers.set('set-cookie', setCookieHeader);
    // }

    return nextResponse;
  } catch (error) {
    console.error("Error in regions API:", error);
    return NextResponse.json([], { status: 500 });
  }
}