"use client";
import { useRef, useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";
import styles from "./page.module.css";
import Map from "@/components/map/Map";
import Icons from "@/components/IconsContainer/Icons";
import Search from "@/components/search/Search";
import PlusMinus from "@/components/plusminus/PlusMinus";
import ResetButton from "@/components/resetButton/ResetButton";
import ResizableDrawer from "@/components/resizableDrawer/ResizableDrawer";
import RegionDrawer from "@/components/RegionDrawer/RegionDrawer";
import DatePickerPopup from "@/components/DatePickerPopup/DatePickerPopup";
import DatePickerTrigger from "@/components/DatePickerPopup/DatePickerTrigger";
import RegionDashboardPopup from "@/components/DashboardPopup/RegionDashboardPopup";
import RegionOperatorsPopup from "@/components/RegionOperatorsPopup/RegionOperatorsPopup";
import TableManager from "@/components/TableManager/TableManager";
import { useTable } from '@/contexts/TableContext';

export default function Home() {
  const mapRef = useRef(null);
  const [selectedCity, setSelectedCity] = useState(null);
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();
  const [tileUrl, setTileUrl] = useState(
    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
  );
  const [isDarkTheme, setIsDarkTheme] = useState(false);
  const [logoutLoading, setLogoutLoading] = useState(false);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [isDrawerClosing, setIsDrawerClosing] = useState(false);
  const [selectedRegion, setSelectedRegion] = useState(null);
  const [isRegionDrawerOpen, setIsRegionDrawerOpen] = useState(false);
  const [isRegionDrawerClosing, setIsRegionDrawerClosing] = useState(false);

  // Состояние для дашборда региона
  const [showRegionDashboard, setShowRegionDashboard] = useState(false);
  const [selectedDashboardRegion, setSelectedDashboardRegion] = useState(null);
  const [dashboardData, setDashboardData] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(false);

  const [showRegionOperators, setShowRegionOperators] = useState(false);
  const [selectedOperatorsRegion, setSelectedOperatorsRegion] = useState(null);

  // Состояние для TableManager
  const [isTableManagerOpen, setIsTableManagerOpen] = useState(false);
// const [tableVersion, setTableVersion] = useState(0);

const { tableVersion } = useTable();

  // Состояние для фильтров даты и региона
  const [filters, setFilters] = useState({
    dateRange: undefined,
    region: undefined
  });

  // Состояние для попапа с календарем
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [isInitialCheckDone, setIsInitialCheckDone] = useState(false);

  // Проверка предпочтений даты пользователя
  const checkDatePreference = useCallback(() => {
    const hasDatePreference = localStorage.getItem("hasDatePreference");
    const savedDateRange = localStorage.getItem("userDateRangePreference");
    const savedRegion = localStorage.getItem("userRegionPreference");

    console.log("Проверка предпочтений:", {
      hasDatePreference,
      savedDateRange,
      savedRegion
    });

    if (!hasDatePreference) {
      console.log("Нет предпочтений даты, показываем попап");
      setTimeout(() => {
        setShowDatePicker(true);
      }, 1000);
      setFilters({ dateRange: undefined, region: undefined });
    } else if (hasDatePreference !== "skipped") {
      const newFilters = {};

      if (savedDateRange) {
        try {
          const dateRange = JSON.parse(savedDateRange);
          newFilters.dateRange = {
            date_from: dateRange.date_from,
            date_to: dateRange.date_to
          };
          console.log("Устанавливаем сохраненный диапазон дат:", dateRange);
        } catch (e) {
          console.error("Ошибка парсинга сохраненного диапазона дат:", e);
          newFilters.dateRange = undefined;
        }
      } else {
        newFilters.dateRange = undefined;
      }

      if (savedRegion && savedRegion !== "") {
        newFilters.region = savedRegion;
        console.log("Устанавливаем сохраненный регион:", savedRegion);
      } else {
        newFilters.region = undefined;
      }

      setFilters(newFilters);
    } else {
      setFilters({ dateRange: undefined, region: undefined });
    }

    setIsInitialCheckDone(true);
  }, []);

  const handleShowRegionOperators = useCallback((regionName) => {
    if (!regionName) {
      console.error("regionName не определен для операторов региона");
      return;
    }
    console.log("Открытие операторов для региона:", regionName);
    setSelectedOperatorsRegion(regionName);
    setShowRegionOperators(true);
  }, []);

  const handleCloseRegionOperators = useCallback(() => {
    setShowRegionOperators(false);
    setSelectedOperatorsRegion(null);
  }, []);

  // Обработчик выбора даты из DatePickerPopup
  const handleDateSelect = useCallback((data) => {
    console.log("Данные из DatePickerPopup:", data);

    const newFilters = {
      dateRange: {
        date_from: data.date_from,
        date_to: data.date_to
      },
      region: data.region
    };

    setFilters(newFilters);

    localStorage.setItem("userDateRangePreference", JSON.stringify({
      date_from: data.date_from,
      date_to: data.date_to
    }));
    localStorage.setItem("userRegionPreference", data.region || "");

    console.log("Фильтры обновлены:", newFilters);
  }, []);

  // Обработчик закрытия попапа с датой
  const handleCloseDatePicker = useCallback(() => {
    setShowDatePicker(false);
  }, []);

  // Обработчик открытия попапа с датой по клику на кнопку
  const handleOpenDatePicker = useCallback(() => {
    console.log("Открытие календаря по клику на кнопку");
    setShowDatePicker(true);
  }, []);

  // Обработчик для показа статистики региона
  const handleShowRegionStatistics = useCallback((regionName) => {
    if (!regionName) {
      console.error("regionName не определен");
      return;
    }
    console.log("Открытие RegionDrawer для региона:", regionName);
    setSelectedRegion(regionName);
    setIsRegionDrawerOpen(true);
  }, []);

  // Обработчик для открытия дашборда региона
  const handleShowRegionDashboard = useCallback(async (regionName) => {
    if (!regionName) {
      console.error("regionName не определен для дашборда");
      return;
    }

    console.log("Открытие дашборда для региона:", regionName);
    setSelectedDashboardRegion(regionName);
    setDashboardLoading(true);
    setShowRegionDashboard(true);

    try {
      const API_URL = process.env.NEXT_PUBLIC_API_URL;
      
      // Строим URL с параметром региона
      const url = new URL(`${API_URL}/dashboard/stats`);
      url.searchParams.append('region', regionName);
      
      // Добавляем фильтры даты, если они есть
      if (filters.dateRange) {
        if (filters.dateRange.date_from) {
          url.searchParams.append('date_from', filters.dateRange.date_from);
        }
        if (filters.dateRange.date_to) {
          url.searchParams.append('date_to', filters.dateRange.date_to);
        }
      }

      console.log("Запрос дашборда региона:", url.toString());

      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      console.log("Данные дашборда региона загружены:", data);
      setDashboardData(data);
    } catch (error) {
      console.error("Ошибка загрузки данных дашборда:", error);
      setDashboardData(null);
    } finally {
      setDashboardLoading(false);
    }
  }, [filters.dateRange]);

  // Обработчик закрытия дашборда региона
  const handleCloseRegionDashboard = useCallback(() => {
    setShowRegionDashboard(false);
    setSelectedDashboardRegion(null);
    setDashboardData(null);
    setDashboardLoading(false);
  }, []);

  // Обработчик закрытия RegionDrawer
  const handleCloseRegionDrawer = useCallback(() => {
    setIsRegionDrawerClosing(true);
    setTimeout(() => {
      setIsRegionDrawerOpen(false);
      setIsRegionDrawerClosing(false);
      setSelectedRegion(null);
    }, 300);
  }, []);

  // Обработчик клика по оверлею для RegionDrawer
  const handleRegionOverlayClick = useCallback(() => {
    handleCloseRegionDrawer();
  }, [handleCloseRegionDrawer]);

  // Обработчики для TableManager
  const handleOpenTableManager = useCallback(() => {
    setIsTableManagerOpen(true);
  }, []);

  const handleCloseTableManager = useCallback(() => {
    setIsTableManagerOpen(false);
  }, []);

  const handleTableSelect = useCallback((tableName) => {
    console.log("Таблица выбрана в page.jsx:", tableName);
  }, []);


  const handleTablesUpdate = useCallback(() => {
    console.log("Список таблиц обновлен");
  }, []);

  // Мемоизируем обработчики
  const handleTileUrlChange = useCallback((newUrl) => {
    setTileUrl(newUrl);
    const isDark = newUrl.includes("dark");
    setIsDarkTheme(isDark);
  }, []);

  const handleCitySelect = useCallback(async (cityName) => {
    setSelectedCity(cityName);

    if (cityName === null) {
      console.log("Поиск очищен");
      if (mapRef.current?.resetMap) {
        mapRef.current.resetMap();
      }
      return;
    }

    try {
      const response = await fetch(
        `http://37.252.22.137:8000/city/${encodeURIComponent(cityName)}`
      );
      if (response.ok) {
        const cityData = await response.json();
        console.log("Данные города:", cityData);

        if (mapRef.current?.updateCityData) {
          mapRef.current.updateCityData(cityData);
        }
      }
    } catch (error) {
      console.error("Ошибка загрузки данных города:", error);
    }
  }, []);

  const handleLogout = useCallback(async () => {
    setLogoutLoading(true);
    try {
      await supabase.auth.signOut();
    } catch (error) {
      console.error("Ошибка при выходе:", error);
      setLogoutLoading(false);
    }
  }, []);

  const checkUser = useCallback(async () => {
    try {
      const {
        data: { session },
      } = await supabase.auth.getSession();

      if (session?.user) {
        await getUserData(session.user);
      } else {
        const localUser = localStorage.getItem("user");
        if (!localUser) {
          router.push("/auth");
          return;
        }
        setUser(JSON.parse(localUser));
      }
    } catch (error) {
      console.error("Error checking user:", error);
      router.push("/auth");
    } finally {
      setLoading(false);
    }
  }, [router]);

  const getUserData = useCallback(async (userData) => {
    try {
      const userInfo = {
        id: userData.id,
        email: userData.email,
        role: userData.role || "authenticated",
        name: userData.user_metadata?.name || userData.email,
      };

      setUser(userInfo);
      localStorage.setItem("user", JSON.stringify(userInfo));
    } catch (error) {
      console.error("Error getting user data:", error);
    }
  }, []);

  useEffect(() => {
    if (isDarkTheme) {
      document.body.setAttribute("data-theme", "dark");
    } else {
      document.body.removeAttribute("data-theme");
    }
  }, [isDarkTheme]);

  useEffect(() => {
    checkUser();

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange(async (event, session) => {
      if (event === "SIGNED_IN" && session) {
        await getUserData(session.user);
      } else if (event === "SIGNED_OUT") {
        setUser(null);
        localStorage.removeItem("user");
        localStorage.removeItem("supabase_token");
        localStorage.removeItem("hasDatePreference");
        localStorage.removeItem("userDateRangePreference");
        localStorage.removeItem("userRegionPreference");
        setFilters({ dateRange: null, region: null });
        router.push("/auth");
      }
    });

    return () => subscription.unsubscribe();
  }, [checkUser, getUserData, router]);

  // Проверяем предпочтения даты после загрузки пользователя
  useEffect(() => {
    if (user && !loading && !isInitialCheckDone) {
      checkDatePreference();
    }
  }, [user, loading, isInitialCheckDone, checkDatePreference]);

  const handleStatsClick = useCallback((statsType = 'main') => {
    if (statsType === 'main') {
      if (isDrawerOpen) {
        setIsDrawerClosing(true);
        setTimeout(() => {
          setIsDrawerOpen(false);
          setIsDrawerClosing(false);
        }, 300);
      } else {
        setIsDrawerOpen(true);
      }
    } else if (statsType === 'secondary') {
      console.log("Secondary stats functionality");
    }
  }, [isDrawerOpen]);

  // Обработчик закрытия drawer
  const handleCloseDrawer = useCallback(() => {
    setIsDrawerClosing(true);
    setTimeout(() => {
      setIsDrawerOpen(false);
      setIsDrawerClosing(false);
    }, 300);
  }, []);

  // Обработчик клика по оверлею
  const handleOverlayClick = useCallback(() => {
    handleCloseDrawer();
  }, [handleCloseDrawer]);

  const getDatePickerTriggerText = useCallback(() => {
    if (!filters.dateRange && !filters.region) {
      return "Выберите период";
    }

    let text = "";

    if (filters.dateRange?.date_from && filters.dateRange?.date_to) {
      text += `${filters.dateRange.date_from} - ${filters.dateRange.date_to}`;
    } else if (filters.dateRange?.date_from) {
      text += `С ${filters.dateRange.date_from}`;
    } else if (filters.dateRange?.date_to) {
      text += `По ${filters.dateRange.date_to}`;
    }

    if (filters.region) {
      if (text) text += " • ";
      text += filters.region;
    }

    return text || "Выберите период";
  }, [filters]);

  if (loading) {
    return (
      <div className={styles.container}>
        <div className={styles.loading}>Загрузка...</div>
      </div>
    );
  }

  if (!user) {
    return null;
  }

  return (
    <div className={styles.container}>
      {/* Первый уровень - карта */}
      <div className={styles.Map}>
<Map
          foundRegions={selectedCity ? [selectedCity] : []}
          ref={mapRef}
          selectedCity={selectedCity}
          tileUrl={tileUrl}
          onTileUrlChange={handleTileUrlChange}
          onShowRegionStatistics={handleShowRegionStatistics}
          onShowRegionDashboard={handleShowRegionDashboard}
          dateRange={filters.dateRange || null}
          selectedRegion={filters.region || null}
          onShowRegionOperators={handleShowRegionOperators}
          tableVersion={tableVersion} 
        />
      </div>

      {/* Второй уровень - поиск и иконки */}
      <div className={styles.overlayContent}>
        <div className={styles.LeftSearchBar}>
          <div className={styles.searchFilter}>
            <Search onCitySelect={handleCitySelect} />
          </div>

          <div className={styles.userPanel}>
            <button
              onClick={handleLogout}
              className={styles.logoutButton}
              disabled={logoutLoading}
            >
              {logoutLoading ? "Выход..." : "Выйти"}
            </button>
          </div>
        </div>
        <div className={styles.iconsContainer}>
          <div className={styles.icons}>
            <div className={styles.icons}>
              <Icons
                onBrushClick={() => {
                  if (mapRef.current?.changeTileLayer) {
                    const newUrl = tileUrl.includes("openstreetmap")
                      ? "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                      : "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
                    mapRef.current.changeTileLayer(newUrl);
                  }
                }}
                onStatsClick={() => handleStatsClick('main')}
                onOpenDatePicker={handleOpenDatePicker}
                selectedDate={getDatePickerTriggerText()}
                dateRange={filters.dateRange || null}
                selectedRegion={null}
                user={user}
                onOpenTableManager={handleOpenTableManager}
              />
            </div>
          </div>
          <div className={styles.PlusMinusReset}>
            <div className={styles.icons}>
              <PlusMinus mapRef={mapRef} />
            </div>
            <div className={styles.icons}>
              <ResetButton mapRef={mapRef} />
            </div>
          </div>
        </div>
      </div>

      {(isDrawerOpen || isDrawerClosing) && (
        <>
          <div
            className={`${styles.drawerOverlay} ${isDrawerClosing ? styles.fadeOut : ''}`}
            onClick={handleOverlayClick}
          />
          <ResizableDrawer
            onClose={handleCloseDrawer}
            isOpen={isDrawerOpen && !isDrawerClosing}
            dateRange={filters.dateRange || null}
            selectedRegion={filters.region || null}
          />
        </>
      )}

      {(isRegionDrawerOpen || isRegionDrawerClosing) && (
        <>
          <div
            className={`${styles.drawerOverlay} ${isRegionDrawerClosing ? styles.fadeOut : ''}`}
            onClick={handleRegionOverlayClick}
          />
          <RegionDrawer
            onClose={handleCloseRegionDrawer}
            isOpen={isRegionDrawerOpen && !isRegionDrawerClosing}
            regionName={selectedRegion}
            dateRange={filters.dateRange || null}
          />
        </>
      )}

      {/* Попап дашборда региона */}
      <RegionDashboardPopup
        isOpen={showRegionDashboard}
        onClose={handleCloseRegionDashboard}
        dashboardData={dashboardData}
        regionName={selectedDashboardRegion}
      />

      {/* Попап выбора даты */}
      <DatePickerPopup
        isOpen={showDatePicker}
        onClose={handleCloseDatePicker}
        onDateSelect={handleDateSelect}
      />

      <RegionOperatorsPopup
        isOpen={showRegionOperators}
        onClose={handleCloseRegionOperators}
        selectedRegion={selectedOperatorsRegion}
        dateRange={filters.dateRange || null}
      />

      {/* Table Manager */}
      <TableManager
        isOpen={isTableManagerOpen}
        onClose={handleCloseTableManager}
        onTableSelect={handleTableSelect}
        onTablesUpdate={handleTablesUpdate}
        user={user}
      />

      {logoutLoading && (
        <div className={styles.logoutOverlay}>
          <div className={styles.spinner}></div>
        </div>
      )}
    </div>
  );
}