"use client";
import { useRef, useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";
import styles from "./page.module.css";
import Map from "@/components/map/Map";
import Icons from "@/components/IconsContainer/Icons";
import Search from "@/components/search/Search";
import PlusMinus from "@/components/plusminus/PlusMinus";
import ResetButton from "@/components/resetButton/ResetButton";
import Filter from "@/components/Filter/Filter";

export default function Home() {
  const mapRef = useRef(null);
  const [selectedCity, setSelectedCity] = useState(null);
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();
  const [tileUrl, setTileUrl] = useState("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png");
  const [isDarkTheme, setIsDarkTheme] = useState(false);
  const [logoutLoading, setLogoutLoading] = useState(false);

  const handleTileUrlChange = (newUrl) => {
    setTileUrl(newUrl);

    // Определяем тему на основе URL тайлов
    const isDark = newUrl.includes("dark");
    setIsDarkTheme(isDark);
  };

  useEffect(() => {
    if (isDarkTheme) {
      document.body.setAttribute('data-theme', 'dark');
    } else {
      document.body.removeAttribute('data-theme');
    }
  }, [isDarkTheme]);

  // Проверяем аутентификацию при загрузке
  useEffect(() => {
    checkUser();

    // Слушаем изменения аутентификации
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
  }, [router]);

  const checkUser = async () => {
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
  };

  const getUserData = async (userData) => {
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
  };

  const handleCitySelect = async (cityName) => {
    setSelectedCity(cityName);

    if (cityName === null) {
      console.log("Поиск очищен");
      if (mapRef.current && mapRef.current.resetMap) {
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

        if (mapRef.current && mapRef.current.updateCityData) {
          mapRef.current.updateCityData(cityData);
        }
      }
    } catch (error) {
      console.error("Ошибка загрузки данных города:", error);
    }
  };

  const handleLogout = async () => {
    setLogoutLoading(true); // Включаем индикатор загрузки
    try {
      await supabase.auth.signOut();
      // После успешного выхода, автоматически произойдет переход на /auth
      // благодаря обработчику onAuthStateChange
    } catch (error) {
      console.error("Ошибка при выходе:", error);
      setLogoutLoading(false); // Выключаем индикатор в случае ошибки
    }
  };

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
            <Filter />
          </div>

          <div className={styles.userPanel}>
            <button 
              onClick={handleLogout} 
              className={styles.logoutButton}
              disabled={logoutLoading} // Отключаем кнопку во время загрузки
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
      {logoutLoading && (
        <div className={styles.logoutOverlay}>
          <div className={styles.spinner}></div>
        </div>
      )}
    </div>
  );
}