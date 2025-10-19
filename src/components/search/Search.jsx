import React, { useState, useRef, useEffect, useCallback } from 'react'
import styles from './search.module.css'
import Image from 'next/image'

const Search = ({ onCitySelect }) => {
  const [searchTerm, setSearchTerm] = useState('')
  const [cities, setCities] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)
  const [allCitiesCache, setAllCitiesCache] = useState([])
  const dropdownRef = useRef(null)
  const inputRef = useRef(null)
  const searchTimeoutRef = useRef(null)

  // Функция для извлечения названия города из объекта
  const getCityName = useCallback((city) => {
    if (typeof city === 'string') return city
    if (city && city.name_ru) return city.name_ru
    if (city && city.name_en) return city.name_en
    return String(city)
  }, [])

  // Загрузка всех городов при первом фокусе
  const loadAllCities = useCallback(async () => {
    if (allCitiesCache.length > 0) {
      console.log('🔄 Используем кэш городов:', allCitiesCache.length)
      return allCitiesCache
    }

    setIsLoading(true)
    try {
      console.log('🌐 Загружаем города с API...')
      const response = await fetch('/api/cities')
      if (response.ok) {
        const data = await response.json()
        console.log('📥 Получены данные с API:', data)
        
        // Используем regions как основной источник
        const citiesData = data.regions || data.cities || []
        console.log('🏙️ Обработанные города:', citiesData.length, citiesData)
        
        setAllCitiesCache(citiesData)
        return citiesData
      }
      console.warn('⚠️ Ответ API не OK')
      return []
    } catch (error) {
      console.error('❌ Ошибка загрузки городов:', error)
      return []
    } finally {
      setIsLoading(false)
    }
  }, [allCitiesCache.length])

  const handleCitySelect = useCallback((city) => {
    const cityName = getCityName(city)
    console.log('🎯 Выбран регион:', cityName, city)
    setSearchTerm(cityName)
    setShowDropdown(false)
    if (inputRef.current) {
      inputRef.current.blur()
    }
    if (onCitySelect) {
      onCitySelect(cityName)
    }
  }, [onCitySelect, getCityName])

  const handleInputChange = useCallback((e) => {
  const value = e.target.value
  console.log('⌨️ Ввод:', value)
  setSearchTerm(value)
  
  // Очищаем предыдущий таймаут
  if (searchTimeoutRef.current) {
    clearTimeout(searchTimeoutRef.current)
  }

  // Устанавливаем новый таймаут для поиска
  searchTimeoutRef.current = setTimeout(async () => {
    if (value.trim() === '') {
      console.log('🔍 Пустой поиск - показываем все регионы')
      // Если поле пустое, показываем все города из кэша
      if (allCitiesCache.length > 0) {
        console.log('📋 Показываем кэшированные регионы:', allCitiesCache.length)
        setCities(allCitiesCache)
        setShowDropdown(true)
      } else {
        console.log('🔄 Кэш пуст - загружаем регионы')
        const cities = await loadAllCities()
        console.log('📋 Загружены регионы для показа:', cities.length)
        setCities(cities)
        setShowDropdown(cities.length > 0)
      }
    } else {
      console.log('🔍 Локальный поиск по тексту:', value)
      
      // ЛОКАЛЬНЫЙ ПОИСК по кэшированным данным
      if (allCitiesCache.length > 0) {
        const filteredCities = allCitiesCache.filter(city => {
          const cityName = getCityName(city).toLowerCase()
          return cityName.includes(value.toLowerCase())
        })
        console.log('🔍 Локально найдено регионов:', filteredCities.length)
        setCities(filteredCities)
        setShowDropdown(filteredCities.length > 0)
      } else {
        // Если кэш пуст, загружаем и потом фильтруем
        console.log('🔄 Кэш пуст - загружаем для поиска')
        setIsLoading(true)
        try {
          const cities = await loadAllCities()
          const filteredCities = cities.filter(city => {
            const cityName = getCityName(city).toLowerCase()
            return cityName.includes(value.toLowerCase())
          })
          console.log('🔍 Найдено регионов после загрузки:', filteredCities.length)
          setCities(filteredCities)
          setShowDropdown(filteredCities.length > 0)
        } catch (error) {
          console.error('❌ Ошибка поиска:', error)
          setCities([])
          setShowDropdown(false)
        } finally {
          setIsLoading(false)
        }
      }
    }
  }, 300)
}, [allCitiesCache, loadAllCities, getCityName])

  const handleInputFocus = useCallback(async () => {
    console.log('👁️ Фокус на инпуте')
    if (allCitiesCache.length > 0) {
      console.log('📋 Показываем кэш при фокусе:', allCitiesCache.length)
      setCities(allCitiesCache)
      setShowDropdown(true)
    } else {
      console.log('🔄 Кэш пуст - загружаем при фокусе')
      setIsLoading(true)
      const cities = await loadAllCities()
      console.log('📋 Загружены регионы при фокусе:', cities.length)
      setCities(cities)
      setShowDropdown(cities.length > 0)
      setIsLoading(false)
    }
  }, [allCitiesCache, loadAllCities])

  const handleClearSearch = useCallback(() => {
    console.log('🧹 Очистка поиска')
    setSearchTerm('')
    setCities(allCitiesCache)
    setShowDropdown(false)
    if (onCitySelect) {
      onCitySelect(null)
    }
    if (inputRef.current) {
      inputRef.current.focus()
    }
  }, [allCitiesCache, onCitySelect])

  const handleDropdownClick = useCallback((e) => {
    e.stopPropagation()
  }, [])

  const handleContainerClick = useCallback((e) => {
    e.stopPropagation()
  }, [])

  // Очистка таймаута при размонтировании
  useEffect(() => {
    return () => {
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current)
      }
    }
  }, [])

  // Закрытие dropdown при клике вне компонента
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false)
      }
    }

    document.addEventListener('click', handleClickOutside)
    return () => document.removeEventListener('click', handleClickOutside)
  }, [])

  // Отладка состояний
  useEffect(() => {
    console.log('📊 Состояние компонента:', {
      searchTerm,
      citiesCount: cities.length,
      isLoading,
      showDropdown,
      allCitiesCacheCount: allCitiesCache.length
    })
  }, [searchTerm, cities, isLoading, showDropdown, allCitiesCache])

  return (
    <div 
      className={styles.searchWrapper} 
      ref={dropdownRef}
      onClick={handleContainerClick}
    >
      <div 
        className={`${styles.searchContainer} ${showDropdown ? styles.searchContainerOpen : ''}`}
        onClick={handleContainerClick}
      >
        <Image src='/svg/search.svg' width={25} height={25} alt='search-icon'/>
        <input 
          ref={inputRef}
          type="text" 
          className={styles.searchInput}
          placeholder="Поиск региона..."
          value={searchTerm}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
          onClick={handleContainerClick}
        />
        {searchTerm && (
          <button 
            className={styles.clearButton}
            onClick={handleClearSearch}
            type="button"
          >
            <Image src='/svg/close.svg' width={16} height={16} alt='clear'/>
          </button>
        )}
        {isLoading && (
          <div className={styles.loader}></div>
        )}
      </div>
      
      <div 
        className={`${styles.dropdown} ${showDropdown ? styles.dropdownOpen : ''}`}
        onClick={handleDropdownClick}
      >
        {cities.map((city, index) => (
          <div 
            key={index}
            className={styles.dropdownItem}
            onClick={() => handleCitySelect(city)}
          >
            {getCityName(city)}
          </div>
        ))}
        {cities.length === 0 && !isLoading && searchTerm && (
          <div className={styles.dropdownItem}>
            Города не найдены
          </div>
        )}
        {cities.length === 0 && !isLoading && !searchTerm && (
          <div className={styles.dropdownItem}>
            Нет доступных городов
          </div>
        )}
      </div>
    </div>
  )
}

export default React.memo(Search)