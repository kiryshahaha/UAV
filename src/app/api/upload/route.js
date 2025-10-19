// app/api/upload/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

export async function POST(request) {
  try {
    const formData = await request.formData();
    const file = formData.get('file');
    const uploadType = formData.get('uploadType') || 'new';

    if (!file) {
      return NextResponse.json(
        { error: "Файл не предоставлен" },
        { status: 400 }
      );
    }

    const backendFormData = new FormData();
    backendFormData.append('file', file);

    const endpoint = uploadType === 'append' ? '/api/upload/append' : '/api/upload';
    
    const response = await fetch(`${API_URL}${endpoint}`, {
      method: 'POST',
      body: backendFormData,
      credentials: 'include',
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const result = await response.json();
    
    // Копируем cookies из ответа бэкенда
    const nextResponse = NextResponse.json(result);
    const setCookieHeader = response.headers.get('set-cookie');
    if (setCookieHeader) {
      nextResponse.headers.set('set-cookie', setCookieHeader);
    }
    
    return nextResponse;
    
  } catch (error) {
    console.error("❌ Ошибка при загрузке файла:", error.message);
    return NextResponse.json(
      { error: "Ошибка при загрузке файла" },
      { status: 500 }
    );
  }
}