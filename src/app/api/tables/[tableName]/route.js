// app/api/tables/[tableName]/route.js
import { NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL;

export async function DELETE(request, { params }) {
  try {
    const { tableName } = params;

    const response = await fetch(`${API_URL}/tables/${tableName}`, {
      method: 'DELETE',
      credentials: 'include',
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const result = await response.json();
    
    return NextResponse.json(result);

  } catch (error) {
    console.error("❌ Ошибка при удалении таблицы:", error.message);
    return NextResponse.json(
      { error: "Ошибка при удалении таблицы" },
      { status: 500 }
    );
  }
}