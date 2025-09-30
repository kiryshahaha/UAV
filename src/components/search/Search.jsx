import React, { useState, useRef, useEffect } from 'react'
import styles from './search.module.css'
import Image from 'next/image'

const Search = ({ onCitySelect }) => {
  const [searchTerm, setSearchTerm] = useState('')
  const [cities, setCities] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)
  const [hasBeenFocused, setHasBeenFocused] = useState(false)
  const [allCitiesCache, setAllCitiesCache] = useState([]) // Кэш для всех городов
  const dropdownRef = useRef(null)
  const inputRef = useRef(null)

  // Загрузка всех городов при фокусе
  const loadAllCities = async () => {
    // Если города уже загружены в кэш, используем их
    if (allCitiesCache.length > 0) {
      setCities(allCitiesCache)
      if (hasBeenFocused) {
        setShowDropdown(true)
      }
      return
    }

    setIsLoading(true)
    try {
      const response = await fetch('/api/cities')
      if (response.ok) {
        const data = await response.json()
        const citiesData = data.cities || []
        setCities(citiesData)
        setAllCitiesCache(citiesData) // Сохраняем в кэш
        if (hasBeenFocused) {
          setShowDropdown(citiesData.length > 0)
        }
      }
    } catch (error) {
      console.error('Ошибка загрузки городов:', error)
      setCities([])
    } finally {
      setIsLoading(false)
    }
  }

  // Поиск городов при изменении текста
  useEffect(() => {
    const searchCities = async () => {
      if (searchTerm.length === 0) {
        // Если поле пустое, показываем все города из кэша
        if (hasBeenFocused) {
          if (allCitiesCache.length > 0) {
            setCities(allCitiesCache)
            setShowDropdown(true)
          } else {
            await loadAllCities()
          }
        }
        return
      }

      setIsLoading(true)
      try {
        const response = await fetch(`/api/cities?search=${encodeURIComponent(searchTerm)}`)
        if (response.ok) {
          const data = await response.json()
          setCities(data.cities || [])
          setShowDropdown(data.cities && data.cities.length > 0)
        }
      } catch (error) {
        console.error('Ошибка поиска городов:', error)
        setCities([])
      } finally {
        setIsLoading(false)
      }
    }

    const timeoutId = setTimeout(searchCities, 500) // Увеличиваем задержку до 500мс
    return () => clearTimeout(timeoutId)
  }, [searchTerm, hasBeenFocused, allCitiesCache])

  // Закрытие dropdown и снятие фокуса при клике вне компонента
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false)
        if (inputRef.current) {
          inputRef.current.blur()
        }
      }
    }

    document.addEventListener('click', handleClickOutside)
    return () => document.removeEventListener('click', handleClickOutside)
  }, [])

  const handleCitySelect = (city) => {
    setSearchTerm(city)
    setShowDropdown(false)
    if (inputRef.current) {
      inputRef.current.blur()
    }
    if (onCitySelect) {
      onCitySelect(city)
    }
  }

  const handleInputChange = (e) => {
    setSearchTerm(e.target.value)
  }

  const handleInputFocus = async () => {
    setHasBeenFocused(true)
    // При фокусе используем кэшированные города или загружаем
    if (allCitiesCache.length > 0) {
      setCities(allCitiesCache)
      setShowDropdown(true)
    } else {
      await loadAllCities()
    }
  }

  const handleInputBlur = () => {
    // Оставляем пустым, так как обрабатываем через handleClickOutside
  }

  const handleClearSearch = () => {
    setSearchTerm('')
    // После очистки возвращаем все города из кэша
    if (allCitiesCache.length > 0) {
      setCities(allCitiesCache)
    }
    setShowDropdown(false)
    if (onCitySelect) {
      onCitySelect(null)
    }
    if (inputRef.current) {
      inputRef.current.focus()
    }
  }

  const handleDropdownClick = (e) => {
    e.stopPropagation()
  }

  const handleContainerClick = (e) => {
    e.stopPropagation()
  }

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
          placeholder="Поиск города..."
          value={searchTerm}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
          onBlur={handleInputBlur}
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
            onClick={(e) => {
              e.stopPropagation()
              handleCitySelect(city)
            }}
          >
            {city}
          </div>
        ))}
        {cities.length === 0 && !isLoading && searchTerm && (
          <div className={styles.dropdownItem}>
            Города не найдены
          </div>
        )}
        {cities.length === 0 && !isLoading && !searchTerm && hasBeenFocused && (
          <div className={styles.dropdownItem}>
            Нет доступных городов
          </div>
        )}
      </div>
    </div>
  )
}

export default Search
