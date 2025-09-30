import { NextResponse } from 'next/server'

const mockCities = [
  "Московский", "Красноярский", "Новосибирский", "Екатеринбургский",
  "Ростовский", "Самарский", "Хабаровский", "Петропавловск-Камчатский"
]

async function searchCities(searchTerm) {
  const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000'
  
  try {
    const url = searchTerm 
      ? `${API_URL}/cities?search=${encodeURIComponent(searchTerm)}`
      : `${API_URL}/cities`
    
    const response = await fetch(url, {
      signal: AbortSignal.timeout(3000),
    })
    
    if (response.ok) {
      const data = await response.json()
      console.log('✅ Города получены с бэкенда:', data.cities)
      return data.cities
    } else {
      throw new Error(`HTTP ${response.status}`)
    }
  } catch (error) {
    console.warn('❌ Backend недоступен, используем mock данные:', error.message)
    // Фильтруем mock данные по поисковому запросу
    return mockCities.filter(city => 
      !searchTerm || city.toLowerCase().includes(searchTerm.toLowerCase())
    ).slice(0, 10)
  }
}

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const searchTerm = searchParams.get('search')?.toLowerCase() || ''

  try {
    const cities = await searchCities(searchTerm)
    
    console.log('Найдено городов:', cities.length)

    return NextResponse.json({ 
      cities: cities,
      total: cities.length
    })
  } catch (error) {
    console.error('Error in cities API:', error)
    const filteredCities = mockCities.filter(city => 
      city.toLowerCase().includes(searchTerm)
    ).slice(0, 10)
    
    return NextResponse.json({ 
      cities: filteredCities,
      total: filteredCities.length
    })
  }
}