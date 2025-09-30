import React, { useState, useRef, useEffect } from 'react'
import styles from './search.module.css'
import Image from 'next/image'

const Search = ({ onCitySelect }) => {
  const [searchTerm, setSearchTerm] = useState('')
  const [cities, setCities] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)
  const dropdownRef = useRef(null)

  // Поиск городов при изменении текста
  useEffect(() => {
    const searchCities = async () => {
      if (searchTerm.length < 2) {
        setCities([])
        setShowDropdown(false)
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

    const timeoutId = setTimeout(searchCities, 300)
    return () => clearTimeout(timeoutId)
  }, [searchTerm])

  // Закрытие dropdown при клике вне компонента
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleCitySelect = (city) => {
    setSearchTerm(city)
    setShowDropdown(false)
    if (onCitySelect) {
      onCitySelect(city)
    }
  }

  const handleInputChange = (e) => {
    setSearchTerm(e.target.value)
  }

  const handleInputFocus = () => {
    if (cities.length > 0) {
      setShowDropdown(true)
    }
  }

  return (
    <div className={styles.searchWrapper} ref={dropdownRef}>
      <div className={styles.searchContainer}>
        <Image src='/svg/search.svg' width={25} height={25} alt='search-icon'/>
        <input 
          type="text" 
          className={styles.searchInput}
          placeholder="Поиск города..."
          value={searchTerm}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
        />
        {isLoading && (
          <div className={styles.loader}></div>
        )}
      </div>
      
      {/* Выпадающий список городов */}
      {showDropdown && (
        <div className={styles.dropdown}>
          {cities.map((city, index) => (
            <div 
              key={index}
              className={styles.dropdownItem}
              onClick={() => handleCitySelect(city)}
            >
              {city}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default Search