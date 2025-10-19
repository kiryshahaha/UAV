// app/api/stats/regions/monthly/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

async function fetchRegionsMonthlyData(dateFrom, dateTo, sessionId, cookieHeader) {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    // Строим URL с параметрами даты
    const url = new URL(`${API_URL}/stats/regions/monthly`);
    if (dateFrom) url.searchParams.append('date_from', dateFrom);
    if (dateTo) url.searchParams.append('date_to', dateTo);

    console.log('📊 Запрос месячной статистики к бэкенду:', url.toString());

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
      console.log("✅ Месячные данные регионов получены с бэкенда:", data.regions?.length || 0);
      return data || { regions: [] };
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (error) {
    console.error("❌ Ошибка при получении месячных данных регионов с бэкенда:", error.message);
    return { regions: [] };
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

    console.log(`📊 Запрос месячной статистики регионов: date_from=${dateFrom}, date_to=${dateTo}, session_id=${sessionId}`);

    const regionsData = await fetchRegionsMonthlyData(dateFrom, dateTo, sessionId, cookieHeader);

    console.log(`✅ Получено регионов с месячной статистикой:`, regionsData.regions?.length || 0);

    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json(regionsData);
    // Note: Бэкенд может вернуть set-cookie, который нужно проксировать
    // const setCookieHeader = response.headers.get('set-cookie');
    // if (setCookieHeader) {
    //   nextResponse.headers.set('set-cookie', setCookieHeader);
    // }

    return nextResponse;
  } catch (error) {
    console.error("Error in regions monthly API:", error);
    return NextResponse.json({ regions: [] }, { status: 500 });
  }
}