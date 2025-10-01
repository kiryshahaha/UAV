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
        `http://localhost:8000/city/${encodeURIComponent(cityName)}`
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

  // Выносим стабильные функции из useEffect
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
        router.push("/auth");
      }
    });

    return () => subscription.unsubscribe();
  }, [checkUser, getUserData, router]);

  const handleStatsClick = useCallback(() => {
    if (isDrawerOpen) {
      // Если drawer открыт, начинаем закрытие
      setIsDrawerClosing(true);
      setTimeout(() => {
        setIsDrawerOpen(false);
        setIsDrawerClosing(false);
      }, 300);
    } else {
      // Если drawer закрыт, открываем
      setIsDrawerOpen(true);
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
        />
      </div>

      {/* Второй уровень - поиск и иконки */}
      <div className={styles.overlayContent}>
        <div className={styles.LeftSearchBar}>
          <div className={styles.searchFilter}>
            <Search onCitySelect={handleCitySelect} />
            {/* <Filter /> */}
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
            <Icons
              onBrushClick={() => {
                if (mapRef.current?.changeTileLayer) {
                  const newUrl = tileUrl.includes("openstreetmap")
                    ? "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                    : "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
                  mapRef.current.changeTileLayer(newUrl);
                }
              }}
              onStatsClick={handleStatsClick}
              user={user}
            />
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
          />
        </>
      )}

      {logoutLoading && (
        <div className={styles.logoutOverlay}>
          <div className={styles.spinner}></div>
        </div>
      )}
    </div>
  );
}
