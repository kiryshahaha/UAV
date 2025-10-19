// app/api/debug/cookies/route.js
import { NextResponse } from "next/server";

export async function GET(request) {
  const cookies = request.headers.get('cookie') || 'No cookies';
  
  console.log("🍪 Cookies received:", cookies);
  
  return NextResponse.json({
    cookies: cookies,
    message: "Check server console for cookie details"
  });
}

export async function POST(request) {
  const cookies = request.headers.get('cookie') || 'No cookies';
  const body = await request.json();
  
  console.log("🍪 Cookies received:", cookies);
  console.log("📦 Body:", body);
  
  // Устанавливаем тестовую cookie
  const response = NextResponse.json({
    cookies: cookies,
    body: body,
    message: "Test cookie set"
  });
  
  response.headers.set('set-cookie', 'test_cookie=hello_from_server; Path=/; HttpOnly');
  
  return response;
}